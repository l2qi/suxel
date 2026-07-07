// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! The append-only orchestration event log.
//!
//! This is **not** the chat transcript (that lives in the agent framework's
//! session, referenced by [`Run::session_ref`](crate::run::Run::session_ref)).
//! These are the runtime's own events: the backbone of durability, audit,
//! replayable progress streams, and debugging.

use crate::ids::RunId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The kind of an orchestration event.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// A run was created.
    RunCreated,
    /// A durable step began executing.
    StepStarted,
    /// A durable step completed and its result was journaled.
    StepCompleted,
    /// A durable step exhausted its retries.
    StepFailed,
    /// A tool call was recorded in the ledger.
    ToolCallRecorded,
    /// The run parked awaiting human approval.
    ApprovalRequested,
    /// An approval decision arrived.
    ApprovalResolved,
    /// A signal was delivered to the run.
    SignalReceived,
    /// A timer/sleep deadline was set.
    TimerSet,
    /// A timer fired.
    TimerFired,
    /// A resource was leased.
    ResourceLeased,
    /// A resource lease was released.
    ResourceReleased,
    /// A child run was spawned.
    ChildRunSpawned,
    /// A child run reached a terminal state (reported to the parent).
    ChildRunFinished,
    /// An artifact was produced.
    ArtifactCreated,
    /// The run completed successfully.
    RunCompleted,
    /// The run failed.
    RunFailed,
    /// The run was cancelled.
    RunCancelled,
}

/// One entry in a run's append-only event log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    /// The run this event belongs to.
    pub run_id: RunId,
    /// Monotonic sequence number within the run, starting at 1.
    pub seq: u64,
    /// The event kind.
    pub kind: EventKind,
    /// Kind-specific JSON payload.
    pub payload: serde_json::Value,
    /// When the event was recorded.
    pub at: DateTime<Utc>,
}
