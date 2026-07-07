// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable signals and human-approval decisions.
//!
//! Signals are how the outside world wakes a parked run: a named, payload-carrying
//! message delivered to a run id. Approvals are delivered as a reserved signal.

use crate::ids::RunId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Reserved signal name used to deliver approval decisions.
pub const APPROVAL_SIGNAL: &str = "__suxel_approval";

/// A durable signal delivered to a run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Signal {
    /// Target run.
    pub run_id: RunId,
    /// Signal name (matched against a run's `Wait::Signal`).
    pub name: String,
    /// Arbitrary JSON payload.
    pub payload: serde_json::Value,
    /// When delivered.
    pub created_at: DateTime<Utc>,
    /// Whether a driver has consumed it.
    pub consumed: bool,
}

/// A human (or policy) approval decision for a single tool call.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    /// Allow this one call.
    Allow,
    /// Allow this call and remember the approval for the rest of the run.
    AllowAlways,
    /// Deny this call.
    Deny,
}

/// The resolution of an approval request, delivered back to a parked run.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApprovalResolution {
    /// The tool call this decision applies to.
    pub tool_call_id: String,
    /// The decision.
    pub decision: ApprovalDecision,
}
