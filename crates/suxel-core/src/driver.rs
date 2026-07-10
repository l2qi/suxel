// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! The agent-framework seam.
//!
//! [`AgentDriver`] is the one trait an adapter (e.g. `suxel-sweet`) implements.
//! Given a [`RunContext`], it advances a run by one unit and returns an
//! [`Advance`] telling the engine what to do next. The driver never touches the
//! queue, status table, or timers directly — it expresses intent; the engine
//! makes it durable.

use crate::artifact::Artifact;
use crate::budget::BudgetUsage;
use crate::error::Result;
use crate::event::EventKind;
use crate::ids::{ArtifactId, ResourceId, RunId};
use crate::resource::{Resource, ResourceKind, ResourceStatus};
use crate::run::{JoinMode, Run, RunStatus};
use crate::signal::{ApprovalResolution, APPROVAL_SIGNAL};
use crate::step::{RetryPolicy, StepKey, StepRecord, StepStatus};
use crate::store::Backend;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// A request to pause a run for human approval of one tool call.
#[derive(Clone, Debug, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ApprovalRequest {
    /// The tool call awaiting approval.
    pub tool_call_id: String,
    /// A human-readable summary of what will happen.
    pub summary: String,
    /// The risk class (free-form; the host's policy interprets it).
    pub risk: String,
}

/// Specification for a child run to spawn.
#[derive(Clone, Debug, Default)]
pub struct ChildSpec {
    /// The child's driver selector.
    pub agent_type: String,
    /// The child's goal.
    pub goal: String,
    /// Structured input.
    pub input: Option<serde_json::Value>,
    /// Session pointer.
    pub session_ref: Option<String>,
    /// Budget slice; `None` inherits a copy of the parent's budget.
    pub budget: Option<crate::budget::Budget>,
}

/// The terminal outcome of a child run, as seen by its parent on join.
#[derive(Clone, Debug)]
pub struct ChildOutcome {
    /// The child run id.
    pub run_id: RunId,
    /// The child's goal.
    pub goal: String,
    /// Terminal status.
    pub status: RunStatus,
    /// Output (when completed).
    pub output: Option<serde_json::Value>,
    /// Error (when failed).
    pub error: Option<String>,
}

/// What a driver wants the engine to do after `advance` returns.
pub enum Advance {
    /// Run again immediately (more work to do this run).
    Continue,
    /// Park until a human approves/denies the given request.
    WaitForApproval {
        /// The pending approval.
        request: ApprovalRequest,
    },
    /// Park until a named signal arrives (optionally with a timeout).
    WaitForSignal {
        /// Signal name to resume on.
        name: String,
        /// Optional deadline: if the signal has not arrived within this duration,
        /// the run resumes anyway with [`RunContext::timed_out`] set true, so the
        /// driver can fail or take another path instead of waiting forever.
        /// Re-waiting restarts the timer.
        timeout: Option<Duration>,
    },
    /// Park until the given instant.
    Sleep {
        /// When to resume.
        until: DateTime<Utc>,
    },
    /// Spawn child runs and park until they join.
    SpawnChildren {
        /// The children to create.
        specs: Vec<ChildSpec>,
        /// How to join them.
        join: JoinMode,
    },
    /// Finish successfully with `output`.
    Complete {
        /// The run's result.
        output: serde_json::Value,
    },
    /// Finish with an error; `retry` re-enqueues the run instead of failing.
    Fail {
        /// Failure message.
        error: String,
        /// Whether to retry the run.
        retry: bool,
    },
}

/// The policy/brain that decides what a run does next.
#[async_trait]
pub trait AgentDriver: Send + Sync {
    /// Advance the run by one unit of work.
    async fn advance(&self, ctx: &mut RunContext) -> Result<Advance>;
}

/// Per-`advance` handle a driver uses to do durable work and read run state.
pub struct RunContext {
    backend: Arc<dyn Backend>,
    run: Run,
    usage_delta: BudgetUsage,
    timed_out: bool,
}

impl RunContext {
    pub(crate) fn new(backend: Arc<dyn Backend>, run: Run, timed_out: bool) -> Self {
        RunContext {
            backend,
            run,
            usage_delta: BudgetUsage::default(),
            timed_out,
        }
    }

    /// Whether this turn was resumed because a [`Advance::WaitForSignal`]'s
    /// `timeout` fired *without* the signal arriving — as opposed to the signal
    /// itself, or a fresh turn. A driver uses this to enforce a real deadline
    /// (fail, or take a different path) instead of re-waiting indefinitely; it is
    /// `false` on a signal-driven or first-turn advance.
    pub fn timed_out(&self) -> bool {
        self.timed_out
    }

    pub(crate) fn into_usage_delta(self) -> BudgetUsage {
        self.usage_delta
    }

    /// The run id.
    pub fn run_id(&self) -> RunId {
        self.run.id
    }

    /// A clone of the storage backend handle. Adapters (e.g. `suxel-sweet`) use
    /// this to journal events or memoize tool calls outside the engine loop.
    pub fn backend(&self) -> Arc<dyn Backend> {
        self.backend.clone()
    }

    /// The full run record (snapshot taken when processing began).
    pub fn run(&self) -> &Run {
        &self.run
    }

    /// The run's goal.
    pub fn goal(&self) -> &str {
        &self.run.goal
    }

    /// The structured input, if any.
    pub fn input(&self) -> Option<&serde_json::Value> {
        self.run.input.as_ref()
    }

    /// The session pointer, if any.
    pub fn session_ref(&self) -> Option<&str> {
        self.run.session_ref.as_deref()
    }

    /// Append an orchestration event. Returns its sequence number.
    pub async fn emit(&self, kind: EventKind, payload: serde_json::Value) -> Result<u64> {
        self.backend.append(self.run.id, kind, payload).await
    }

    /// Charge LLM tokens against the run's budget.
    pub fn charge_tokens(&mut self, n: u64) {
        self.usage_delta.tokens += n;
    }

    /// Charge cost (USD) against the run's budget.
    pub fn charge_cost(&mut self, usd: f64) {
        self.usage_delta.cost_usd += usd;
    }

    /// Charge one tool call against the run's budget.
    pub fn charge_tool_call(&mut self) {
        self.usage_delta.tool_calls += 1;
    }

    /// Take the payload of the earliest unconsumed signal with `name`, if present.
    /// Consumes only that one signal — any other delivered signals (including
    /// further ones with the same name) stay unconsumed for a later take.
    pub async fn take_signal(&mut self, name: &str) -> Result<Option<serde_json::Value>> {
        Ok(self
            .backend
            .take_signal(self.run.id, name)
            .await?
            .map(|s| s.payload))
    }

    /// Take all approval resolutions delivered since the run parked. A payload
    /// that fails to decode surfaces as an error rather than being silently
    /// dropped (which would leave the run parked forever).
    pub async fn take_approvals(&mut self) -> Result<Vec<ApprovalResolution>> {
        let mut out = Vec::new();
        while let Some(s) = self
            .backend
            .take_signal(self.run.id, APPROVAL_SIGNAL)
            .await?
        {
            out.push(serde_json::from_value(s.payload)?);
        }
        Ok(out)
    }

    /// The terminal outcomes of this run's children (for a join).
    pub async fn children(&self) -> Result<Vec<ChildOutcome>> {
        let kids = self.backend.list_children(self.run.id).await?;
        Ok(kids
            .into_iter()
            .map(|c| ChildOutcome {
                run_id: c.id,
                goal: c.goal,
                status: c.status,
                output: c.output,
                error: c.error,
            })
            .collect())
    }

    /// Lease a resource for this run, recording it durably. Returns its id, which
    /// a recovering worker can use to re-attach to the same live resource.
    pub async fn lease_resource(
        &self,
        kind: ResourceKind,
        metadata: serde_json::Value,
        lease_expires_at: Option<DateTime<Utc>>,
    ) -> Result<ResourceId> {
        let resource = Resource {
            id: ResourceId::new(),
            run_id: self.run.id,
            kind,
            status: ResourceStatus::Active,
            metadata,
            lease_expires_at,
            created_at: Utc::now(),
        };
        self.backend.lease_resource(&resource).await?;
        self.emit(
            EventKind::ResourceLeased,
            serde_json::json!({ "resource_id": resource.id, "kind": kind }),
        )
        .await?;
        Ok(resource.id)
    }

    /// Extend a resource's lease (heartbeat/keepalive).
    pub async fn heartbeat_resource(
        &self,
        id: ResourceId,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        self.backend.heartbeat(id, expires_at).await
    }

    /// Release a leased resource.
    pub async fn release_resource(&self, id: ResourceId) -> Result<()> {
        self.backend.release_resource(id).await?;
        self.emit(
            EventKind::ResourceReleased,
            serde_json::json!({ "resource_id": id }),
        )
        .await
        .map(|_| ())
    }

    /// List the resources currently leased to this run.
    pub async fn resources(&self) -> Result<Vec<Resource>> {
        self.backend.list_resources(self.run.id).await
    }

    /// Record an artifact produced by this run. Returns its id.
    pub async fn put_artifact(
        &self,
        kind: impl Into<String>,
        uri: impl Into<String>,
        metadata: serde_json::Value,
    ) -> Result<ArtifactId> {
        let artifact = Artifact {
            id: ArtifactId::new(),
            run_id: self.run.id,
            kind: kind.into(),
            uri: uri.into(),
            metadata,
            created_at: Utc::now(),
        };
        self.backend.put_artifact(&artifact).await?;
        self.emit(
            EventKind::ArtifactCreated,
            serde_json::json!({ "artifact_id": artifact.id, "kind": artifact.kind }),
        )
        .await?;
        Ok(artifact.id)
    }

    /// Run a durable step. If a completed result is already journaled under `key`,
    /// it is returned without re-running `f` — the "never repeat a *journaled*
    /// side effect" guarantee. Otherwise `f` runs with `retry` backoff and its
    /// result is journaled before returning.
    ///
    /// Note the one unavoidable window: if the process crashes after `f` succeeds
    /// but before its result is journaled, replay re-invokes `f`. So an opaque
    /// side effect is at-least-once; make `f` idempotent / retry-safe (or itself
    /// transactional) if that matters.
    pub async fn durable_step<T, F, Fut>(
        &mut self,
        key: impl Into<StepKey>,
        retry: RetryPolicy,
        mut f: F,
    ) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = std::result::Result<T, String>> + Send,
    {
        let key = key.into();
        if let Some(rec) = self.backend.lookup_step(self.run.id, &key.0).await? {
            if rec.status == StepStatus::Completed {
                let out = rec.output.unwrap_or(serde_json::Value::Null);
                return Ok(serde_json::from_value(out)?);
            }
        }
        self.emit(EventKind::StepStarted, serde_json::json!({ "key": key.0 }))
            .await?;

        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match f().await {
                Ok(val) => {
                    let payload = serde_json::to_value(&val)?;
                    self.backend
                        .record_step(&StepRecord {
                            run_id: self.run.id,
                            key: key.clone(),
                            status: StepStatus::Completed,
                            output: Some(payload),
                            error: None,
                            attempts: attempt,
                            updated_at: Utc::now(),
                        })
                        .await?;
                    self.emit(
                        EventKind::StepCompleted,
                        serde_json::json!({ "key": key.0, "attempts": attempt }),
                    )
                    .await?;
                    return Ok(val);
                }
                Err(e) => {
                    if attempt >= retry.max_attempts {
                        self.backend
                            .record_step(&StepRecord {
                                run_id: self.run.id,
                                key: key.clone(),
                                status: StepStatus::Failed,
                                output: None,
                                error: Some(e.clone()),
                                attempts: attempt,
                                updated_at: Utc::now(),
                            })
                            .await?;
                        self.emit(
                            EventKind::StepFailed,
                            serde_json::json!({ "key": key.0, "attempts": attempt, "error": e }),
                        )
                        .await?;
                        return Err(crate::error::Error::StepExhausted {
                            key: key.0,
                            attempts: attempt,
                            message: e,
                        });
                    }
                    let delay = retry.backoff_for(attempt);
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
    }
}
