// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable lifecycle for remote execution sandboxes (E2B, Firecracker, …).
//!
//! The runtime owns the *lifecycle*; the agent framework owns the *interface*
//! (its `Sandbox`/`CommandRunner`/`Filesystem` traits, bridged in an adapter
//! crate). This module is provider-agnostic: it provisions a sandbox **once**
//! (memoized by a durable step) and leases it as a [`ResourceKind::CodeSandbox`]
//! so a recovering worker **re-attaches** to the same live sandbox by id instead
//! of creating a new one, and **reaps** it on teardown so a crashed or cancelled
//! run never leaks a paid VM.

use crate::driver::RunContext;
use crate::error::Result;
use crate::ids::RunId;
use crate::resource::{Resource, ResourceKind, ResourceStatus};
use crate::step::RetryPolicy;
use crate::store::Backend;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// A freshly created sandbox: its provider-assigned id plus opaque, provider-
/// specific **connection metadata** (endpoint, domain, access token, …) that a
/// recovering worker needs to re-attach. The runtime persists this verbatim on
/// the lease and never interprets it; the provider's adapter reads it back.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SandboxHandle {
    /// Provider-assigned sandbox id (the durable re-attach handle).
    pub id: String,
    /// Provider-specific connection details, persisted on the lease. `Null` is fine.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl SandboxHandle {
    /// A handle with no extra connection metadata.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            metadata: serde_json::Value::Null,
        }
    }

    /// A handle carrying provider connection metadata.
    pub fn with_metadata(id: impl Into<String>, metadata: serde_json::Value) -> Self {
        Self {
            id: id.into(),
            metadata,
        }
    }
}

/// A remote-sandbox provider's lifecycle operations. Concrete providers (E2B,
/// Firecracker, Modal, …) implement this over their HTTP API/SDK. Errors are
/// stringified so providers stay dependency-free here.
#[async_trait]
pub trait SandboxProvider: Send + Sync {
    /// Create a sandbox and return its [`SandboxHandle`] (id + connection metadata).
    async fn create(&self) -> std::result::Result<SandboxHandle, String>;

    /// Extend the sandbox's keepalive/timeout (heartbeat). Default: no-op.
    async fn keepalive(&self, _sandbox_id: &str) -> std::result::Result<(), String> {
        Ok(())
    }

    /// Destroy the sandbox.
    async fn destroy(&self, sandbox_id: &str) -> std::result::Result<(), String>;
}

/// Provision a sandbox for the current run, durably.
///
/// If the run already holds an active sandbox lease, re-attach to it (returning
/// the same id). Otherwise create one via `provider` inside a memoized durable
/// step and lease it as a [`ResourceKind::CodeSandbox`]. Either way `create`
/// runs **at most once** across crashes and retries.
pub async fn provision_sandbox(
    ctx: &mut RunContext,
    provider: &dyn SandboxProvider,
) -> Result<String> {
    // Re-attach to an existing sandbox if the run already leased one.
    for r in ctx.resources().await? {
        if r.kind == ResourceKind::CodeSandbox && r.status == ResourceStatus::Active {
            if let Some(id) = r.metadata.get("sandbox_id").and_then(|v| v.as_str()) {
                return Ok(id.to_string());
            }
        }
    }
    // First time: create (memoized) and record the lease, persisting the
    // provider's connection metadata alongside the id for re-attach.
    let handle: SandboxHandle = ctx
        .durable_step("provision_sandbox", RetryPolicy::default(), || async {
            provider.create().await
        })
        .await?;
    let mut metadata = match handle.metadata {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };
    metadata.insert("sandbox_id".to_string(), json!(handle.id));
    ctx.lease_resource(
        ResourceKind::CodeSandbox,
        serde_json::Value::Object(metadata),
        None,
    )
    .await?;
    Ok(handle.id)
}

/// The run's active code-sandbox resource, if any. Carries the provider's
/// connection metadata (`sandbox_id` plus whatever the provider persisted), so a
/// provider adapter can rebuild its client after a crash without re-provisioning.
pub async fn sandbox_resource(ctx: &RunContext) -> Result<Option<Resource>> {
    sandbox_resource_of(&*ctx.backend(), ctx.run_id()).await
}

/// The active code-sandbox resource leased to an **arbitrary** run (typically a
/// parent "project" run), read straight from the backend. This is the re-attach
/// seam for a child run that wants to use a sandbox its parent owns the lease on,
/// without provisioning its own or copying the connection metadata into its input.
pub async fn sandbox_resource_of(backend: &dyn Backend, run_id: RunId) -> Result<Option<Resource>> {
    for r in backend.list_resources(run_id).await? {
        if r.kind == ResourceKind::CodeSandbox && r.status == ResourceStatus::Active {
            return Ok(Some(r));
        }
    }
    Ok(None)
}

/// Destroy and release every sandbox leased to the run — call on completion,
/// cancellation, or cleanup so no paid VM is left running.
///
/// Call this at **teardown only**. [`provision_sandbox`] memoizes its create in a
/// durable step, so calling `provision_sandbox` again *after* a reap returns the
/// already-destroyed sandbox id from the journal instead of creating a fresh VM —
/// don't reap and then re-provision within the same run.
pub async fn reap_sandboxes(ctx: &RunContext, provider: &dyn SandboxProvider) -> Result<()> {
    for r in ctx.resources().await? {
        if r.kind == ResourceKind::CodeSandbox && r.status == ResourceStatus::Active {
            if let Some(id) = r.metadata.get("sandbox_id").and_then(|v| v.as_str()) {
                match provider.destroy(id).await {
                    // Release the lease only after the VM is actually gone.
                    Ok(()) => ctx.release_resource(r.id).await?,
                    // Keep the lease Active so a later reap retries destroy —
                    // releasing now would discard the only handle to a live,
                    // billable VM and guarantee a leak on a transient failure.
                    Err(e) => tracing::warn!(
                        sandbox = id,
                        error = %e,
                        "sandbox destroy failed; keeping lease for a later reap"
                    ),
                }
            }
        }
    }
    Ok(())
}
