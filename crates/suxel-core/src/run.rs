// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! The [`Run`] — one durable attempt to accomplish a goal — and its state.

use crate::budget::{Budget, BudgetUsage};
use crate::ids::RunId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Lifecycle state of a run.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Created, enqueued, not yet picked up.
    Pending,
    /// Currently being advanced by a worker.
    Running,
    /// Parked awaiting a human approval decision.
    WaitingApproval,
    /// Parked awaiting a named signal.
    WaitingSignal,
    /// Parked until a timer fires.
    WaitingTimer,
    /// Finished successfully.
    Completed,
    /// Finished with an error.
    Failed,
    /// Cancelled by the caller (or a cancelled ancestor).
    Cancelled,
}

impl RunStatus {
    /// Whether the run has reached a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
        )
    }
}

/// How a parent waits on its spawned children.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JoinMode {
    /// Wake the parent only when every child reaches a terminal state.
    All,
    /// Wake the parent on the first child to finish; cancel the rest.
    Any,
    /// Wake the parent once `n` children have completed successfully (or all are terminal).
    Quorum(u32),
}

/// What a parked run is waiting for. Drives how the engine resumes it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Wait {
    /// A named signal.
    Signal {
        /// The signal name to resume on.
        name: String,
        /// Deadline for a `WaitForSignal { timeout }`; once it has elapsed the run
        /// resumes as timed out. `None` waits indefinitely.
        #[serde(default)]
        deadline: Option<DateTime<Utc>>,
    },
    /// A human approval decision.
    Approval,
    /// A timer deadline.
    Timer,
    /// A set of child runs, joined by `mode`.
    ChildJoin {
        /// The join policy.
        mode: JoinMode,
    },
}

/// One durable attempt to accomplish a goal.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Run {
    /// Unique id.
    pub id: RunId,
    /// Parent run, if this is a child.
    pub parent_id: Option<RunId>,
    /// Selects which `AgentDriver` advances this run.
    pub agent_type: String,
    /// The natural-language goal.
    pub goal: String,
    /// Structured input for the driver, if any.
    pub input: Option<serde_json::Value>,
    /// Current lifecycle state.
    pub status: RunStatus,
    /// What this run is parked on (when `status` is a `Waiting*` variant).
    pub waiting: Option<Wait>,
    /// Opaque pointer to a conversation/session owned by the agent framework.
    pub session_ref: Option<String>,
    /// Resource ceilings.
    pub budget: Budget,
    /// Running usage charged against `budget`.
    pub usage: BudgetUsage,
    /// Final output (when `Completed`).
    pub output: Option<serde_json::Value>,
    /// Failure message (when `Failed`).
    pub error: Option<String>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last-update timestamp.
    pub updated_at: DateTime<Utc>,
}

/// Parameters for creating a new run.
#[derive(Clone, Debug, Default)]
pub struct RunSpec {
    /// Selects the driver.
    pub agent_type: String,
    /// The goal.
    pub goal: String,
    /// Structured input, if any.
    pub input: Option<serde_json::Value>,
    /// Session pointer, if any.
    pub session_ref: Option<String>,
    /// Budget ceilings.
    pub budget: Budget,
    /// Parent run, if spawned as a child.
    pub parent_id: Option<RunId>,
}

impl Run {
    /// Build a fresh `Pending` run from a spec at time `now`.
    pub fn from_spec(spec: RunSpec, now: DateTime<Utc>) -> Self {
        Run {
            id: RunId::new(),
            parent_id: spec.parent_id,
            agent_type: spec.agent_type,
            goal: spec.goal,
            input: spec.input,
            status: RunStatus::Pending,
            waiting: None,
            session_ref: spec.session_ref,
            budget: spec.budget,
            usage: BudgetUsage::default(),
            output: None,
            error: None,
            created_at: now,
            updated_at: now,
        }
    }
}
