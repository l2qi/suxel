// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! The durable sandbox lifecycle: provision once, re-attach on resume, reap.

use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use suxel_core::driver::{Advance, AgentDriver, RunContext};
use suxel_core::resource::ResourceStatus;
use suxel_core::sandbox::{
    provision_sandbox, reap_sandboxes, sandbox_resource, sandbox_resource_of, SandboxHandle,
    SandboxProvider,
};
use suxel_core::store::ResourceStore;
use suxel_core::{Engine, InMemoryBackend, RunSpec, RunStatus};

/// A fake provider that counts create/destroy calls.
struct MockProvider {
    created: AtomicUsize,
    destroyed: AtomicUsize,
}

#[async_trait]
impl SandboxProvider for MockProvider {
    async fn create(&self) -> Result<SandboxHandle, String> {
        let n = self.created.fetch_add(1, Ordering::SeqCst);
        // Carry connection metadata so the test can prove it persists + re-attaches.
        Ok(SandboxHandle::with_metadata(
            format!("sb-{n}"),
            serde_json::json!({ "domain": "mock.dev" }),
        ))
    }
    async fn destroy(&self, _sandbox_id: &str) -> Result<(), String> {
        self.destroyed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Provisions a sandbox, parks for a signal, then (on resume) re-attaches and reaps.
struct SandboxDriver {
    provider: Arc<MockProvider>,
}

#[async_trait]
impl AgentDriver for SandboxDriver {
    async fn advance(&self, ctx: &mut RunContext) -> suxel_core::Result<Advance> {
        let id = provision_sandbox(ctx, &*self.provider).await?;
        if ctx.take_signal("resume").await?.is_some() {
            // On resume, the persisted connection metadata is readable — this is
            // what a provider adapter uses to rebuild its client without a new VM.
            let domain = sandbox_resource(ctx).await?.and_then(|r| {
                r.metadata
                    .get("domain")
                    .and_then(|v| v.as_str().map(String::from))
            });
            reap_sandboxes(ctx, &*self.provider).await?;
            Ok(Advance::Complete {
                output: serde_json::json!({ "sandbox": id, "domain": domain }),
            })
        } else {
            Ok(Advance::WaitForSignal {
                name: "resume".into(),
                timeout: None,
            })
        }
    }
}

#[tokio::test]
async fn provisions_once_reattaches_then_reaps() {
    let provider = Arc::new(MockProvider {
        created: AtomicUsize::new(0),
        destroyed: AtomicUsize::new(0),
    });
    let engine = Engine::new(Arc::new(InMemoryBackend::new())).with_driver(
        "sandboxed",
        Arc::new(SandboxDriver {
            provider: provider.clone(),
        }),
    );
    let id = engine
        .create_run(RunSpec {
            agent_type: "sandboxed".into(),
            ..Default::default()
        })
        .await
        .unwrap();

    // Advance 1: provisions the sandbox (one create), then parks.
    engine.run_until_idle("w").await.unwrap();
    assert_eq!(provider.created.load(Ordering::SeqCst), 1);

    // Resume: provision re-attaches to the leased sandbox (no new create), reaps.
    engine
        .signal(id, "resume", serde_json::json!({}))
        .await
        .unwrap();
    engine.run_until_idle("w").await.unwrap();

    let run = engine.get_run(id).await.unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    let output = run.output.unwrap();
    assert_eq!(output["sandbox"], "sb-0");
    // The provider's connection metadata survived the lease + the re-attach.
    assert_eq!(output["domain"], "mock.dev");
    assert_eq!(
        provider.created.load(Ordering::SeqCst),
        1,
        "sandbox was created more than once"
    );
    assert_eq!(
        provider.destroyed.load(Ordering::SeqCst),
        1,
        "sandbox was not reaped"
    );
}

/// A provider whose `destroy` always fails, to prove reaping does not discard
/// the lease (and thus the handle to a live, billable VM) on a teardown failure.
struct FailingDestroyProvider {
    destroy_attempts: AtomicUsize,
}

#[async_trait]
impl SandboxProvider for FailingDestroyProvider {
    async fn create(&self) -> Result<SandboxHandle, String> {
        Ok(SandboxHandle::with_metadata(
            "sb-x",
            serde_json::json!({ "domain": "mock.dev" }),
        ))
    }
    async fn destroy(&self, _sandbox_id: &str) -> Result<(), String> {
        self.destroy_attempts.fetch_add(1, Ordering::SeqCst);
        Err("destroy failed".into())
    }
}

struct ReapDriver {
    provider: Arc<FailingDestroyProvider>,
}

#[async_trait]
impl AgentDriver for ReapDriver {
    async fn advance(&self, ctx: &mut RunContext) -> suxel_core::Result<Advance> {
        provision_sandbox(ctx, &*self.provider).await?;
        reap_sandboxes(ctx, &*self.provider).await?;
        Ok(Advance::Complete {
            output: serde_json::json!({}),
        })
    }
}

#[tokio::test]
async fn reap_keeps_lease_when_destroy_fails() {
    let provider = Arc::new(FailingDestroyProvider {
        destroy_attempts: AtomicUsize::new(0),
    });
    let backend = Arc::new(InMemoryBackend::new());
    let engine = Engine::new(backend.clone()).with_driver(
        "reap",
        Arc::new(ReapDriver {
            provider: provider.clone(),
        }),
    );
    let id = engine
        .create_run(RunSpec {
            agent_type: "reap".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle("w").await.unwrap();

    assert_eq!(
        provider.destroy_attempts.load(Ordering::SeqCst),
        1,
        "reap must attempt destroy"
    );
    // destroy failed, so the lease stays Active — the handle is retained for a
    // later reap instead of being discarded (which would leak the VM).
    let resources = backend.list_resources(id).await.unwrap();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].status, ResourceStatus::Active);
    assert_eq!(resources[0].metadata["sandbox_id"], "sb-x");
}

#[tokio::test]
async fn sandbox_resource_of_reads_a_runs_lease() {
    let provider = Arc::new(MockProvider {
        created: AtomicUsize::new(0),
        destroyed: AtomicUsize::new(0),
    });
    let backend = Arc::new(InMemoryBackend::new());
    let engine = Engine::new(backend.clone()).with_driver(
        "sandboxed",
        Arc::new(SandboxDriver {
            provider: provider.clone(),
        }),
    );
    let id = engine
        .create_run(RunSpec {
            agent_type: "sandboxed".into(),
            ..Default::default()
        })
        .await
        .unwrap();

    // First advance provisions + leases the sandbox, then parks for the signal.
    engine.run_until_idle("w").await.unwrap();

    // A *different* caller can read that run's sandbox lease by run id — the
    // re-attach seam a child "coding-turn" uses against its parent "project".
    let res = sandbox_resource_of(&*backend, id)
        .await
        .unwrap()
        .expect("a code sandbox is leased to the run");
    assert_eq!(res.metadata["sandbox_id"], "sb-0");
    assert_eq!(res.metadata["domain"], "mock.dev");
}
