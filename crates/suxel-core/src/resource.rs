// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Leased resources: the durable lifecycle around things a run owns while it
//! executes (cloud sandboxes, browser sessions, DB connections, …).
//!
//! The runtime owns the *lifecycle* (provision, heartbeat, re-attach, reap); the
//! agent framework owns the *interface* the agent actually uses.

use crate::ids::{ResourceId, RunId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The class of a leased resource.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// A cloud code-execution sandbox (e.g. an E2B microVM).
    CodeSandbox,
    /// A shell session.
    ShellSession,
    /// A headless/remote browser session.
    BrowserSession,
    /// A working directory / file workspace.
    FileWorkspace,
    /// A database connection.
    DatabaseConnection,
    /// An external API session/token.
    ExternalApi,
    /// A pending human-approval gate.
    HumanApproval,
    /// Anything else.
    Other,
}

/// Lease state of a resource.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceStatus {
    /// Held by the run.
    Active,
    /// Released cleanly.
    Released,
    /// Lease expired (candidate for reaping).
    Expired,
}

/// A resource leased to a run for the duration of (part of) its execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resource {
    /// Unique id.
    pub id: ResourceId,
    /// Owning run.
    pub run_id: RunId,
    /// Resource class.
    pub kind: ResourceKind,
    /// Lease state.
    pub status: ResourceStatus,
    /// Provider-specific handle (e.g. `{ "sandbox_id": "...", "endpoint": "..." }`).
    pub metadata: serde_json::Value,
    /// When the lease expires (for heartbeat/reaping). `None` = no expiry.
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
}
