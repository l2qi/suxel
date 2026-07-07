// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! # suxel-core
//!
//! A generic, agent-framework-agnostic **durable execution runtime** for
//! tool-using autonomous agents. Temporal-inspired, but *agent-run-first*.
//!
//! The runtime's unit is an **[`Run`]** — one durable, resumable attempt to
//! accomplish a goal. An **[`AgentDriver`]** (implemented by an adapter such as
//! `suxel-sweet`) advances a run one step at a time; the **[`Engine`]** makes
//! every transition durable via an append-only [event log](crate::event), an
//! idempotent [durable-step](crate::step) journal, a leased work
//! [queue](crate::store::Queue), and durable [signals](crate::signal)/approvals/timers
//! and child runs.
//!
//! This crate deliberately knows nothing about LLMs, sessions, or any specific
//! agent framework — that lives behind the [`AgentDriver`] seam.
//!
//! ```no_run
//! use std::sync::Arc;
//! use suxel_core::{Engine, InMemoryBackend, RunSpec};
//!
//! # async fn demo(driver: Arc<dyn suxel_core::AgentDriver>) -> suxel_core::Result<()> {
//! let backend = Arc::new(InMemoryBackend::new());
//! let engine = Engine::new(backend).with_driver("echo", driver);
//! let run_id = engine.create_run(RunSpec {
//!     agent_type: "echo".into(),
//!     goal: "say hello".into(),
//!     ..Default::default()
//! }).await?;
//! engine.run_until_idle("worker-1").await?;
//! let run = engine.get_run(run_id).await?;
//! assert!(run.status.is_terminal());
//! # Ok(()) }
//! ```

pub mod artifact;
pub mod budget;
pub mod driver;
pub mod engine;
pub mod error;
pub mod event;
pub mod ids;
pub mod mem;
pub mod resource;
pub mod run;
pub mod sandbox;
pub mod signal;
pub mod step;
pub mod store;

#[cfg(feature = "testkit")]
pub mod testkit;

pub use artifact::Artifact;
pub use budget::{Budget, BudgetUsage};
pub use driver::{Advance, AgentDriver, ApprovalRequest, ChildOutcome, ChildSpec, RunContext};
pub use engine::Engine;
pub use error::{Error, Result};
pub use event::{Event, EventKind};
pub use ids::{ArtifactId, ResourceId, RunId};
pub use mem::InMemoryBackend;
pub use resource::{Resource, ResourceKind, ResourceStatus};
pub use run::{JoinMode, Run, RunSpec, RunStatus, Wait};
pub use sandbox::{
    provision_sandbox, reap_sandboxes, sandbox_resource, sandbox_resource_of, SandboxHandle,
    SandboxProvider,
};
pub use signal::{ApprovalDecision, ApprovalResolution, Signal, APPROVAL_SIGNAL};
pub use step::{RetryPolicy, StepKey, StepRecord, StepStatus};
pub use store::{
    ArtifactStore, Backend, Clock, EventLog, Lease, Queue, ResourceStore, RunStore, SignalStore,
    StepStore, SystemClock,
};
