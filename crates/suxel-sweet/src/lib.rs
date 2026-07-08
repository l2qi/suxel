// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Adapter that runs **Sweet** agents on the **Suxel** durable runtime.
//!
//! This is the only crate that knows about Sweet. It provides:
//!
//! - [`SweetAgentDriver`] — implements [`suxel_core::AgentDriver`]. Given a
//!   factory that builds a Sweet [`Agent`] for a run, it runs one turn and maps
//!   the [`TurnResult`] to a Suxel [`Advance`].
//! - [`DurableIo`] — a Sweet [`AgentIo`] that journals tool calls/results into
//!   the Suxel event log so the UI and audit trail are the same data.
//! - [`wrap_tool`] — decorates a Sweet [`ToolSpec`] so its handler memoizes its
//!   result in the durable-step journal, giving crash-safe "never repeat a
//!   completed side effect" tool execution **without forking Sweet's loop**.
//!
//! Durable human-in-the-loop is supported: [`SweetAgentDriver`] runs the turn
//! via Sweet's `step_stream_interruptible`, maps a paused turn to
//! [`Advance::WaitForApproval`], and on the next advance resumes the rehydrated
//! agent via `resume_with_approvals` — parking the run for a human decision and
//! resuming **without re-invoking the LLM** for the already-produced turn.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use sweet_agent::{Agent, AgentIo, TurnOutcome, TurnResult};
use sweet_core::message::{ContentBlock, Message, ToolCall};
use sweet_core::model::Model;
use sweet_core::permission::{ApprovalDecision as SweetApproval, ToolRisk};
use sweet_core::tool::{ToolError, ToolHandler, ToolOutput, ToolSpec};
use sweet_core::Session;

use suxel_core::driver::{Advance, AgentDriver, ApprovalRequest, RunContext};
use suxel_core::error::{Error, Result};
use suxel_core::event::EventKind;
use suxel_core::ids::RunId;
use suxel_core::signal::ApprovalDecision;
use suxel_core::step::{StepKey, StepRecord, StepStatus};
use suxel_core::store::Backend;

/// What the agent factory is given to build (or rehydrate) an agent for a run.
pub struct BuildContext {
    /// The run being advanced.
    pub run_id: RunId,
    /// The run's goal (typically the user message for this turn).
    pub goal: String,
    /// Structured input, if any.
    pub input: Option<serde_json::Value>,
    /// The opaque session pointer (e.g. a Sweet `SqliteSession` id) to rehydrate.
    pub session_ref: Option<String>,
    /// The durable backend, for wrapping tools with [`wrap_tool`].
    pub backend: Arc<dyn Backend>,
}

/// Builds (or rehydrates from `session_ref`) the Sweet agent for a run.
pub type AgentFactory =
    Arc<dyn Fn(BuildContext) -> std::result::Result<Agent<Box<dyn Model>>, String> + Send + Sync>;

/// A [`suxel_core::AgentDriver`] that advances a run by running one Sweet turn.
pub struct SweetAgentDriver {
    factory: AgentFactory,
}

impl SweetAgentDriver {
    /// Build a driver from an agent factory.
    pub fn new(factory: AgentFactory) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl AgentDriver for SweetAgentDriver {
    async fn advance(&self, ctx: &mut RunContext) -> Result<Advance> {
        let build = BuildContext {
            run_id: ctx.run_id(),
            goal: ctx.goal().to_string(),
            input: ctx.input().cloned(),
            session_ref: ctx.session_ref().map(str::to_string),
            backend: ctx.backend(),
        };
        let mut agent = (self.factory)(build).map_err(Error::driver)?;

        // Approval decisions delivered since the run parked (empty on a fresh turn).
        let approvals: HashMap<String, ApprovalDecision> = ctx
            .take_approvals()
            .await?
            .into_iter()
            .map(|r| (r.tool_call_id, r.decision))
            .collect();
        let mut io = DurableIo {
            backend: ctx.backend(),
            run_id: ctx.run_id(),
            approvals,
        };

        // If the rehydrated session has a paused turn, finish it; else start one.
        let outcome = if agent.has_pending_approvals() {
            agent.resume_with_approvals(&mut io).await
        } else {
            agent
                .step_stream_interruptible(ctx.goal().to_string(), &mut io)
                .await
        }
        .map_err(|e| Error::driver(e.to_string()))?;

        Ok(match outcome {
            TurnOutcome::Turn(TurnResult::Message(m)) => Advance::Complete {
                output: serde_json::json!({ "text": m.text_content() }),
            },
            TurnOutcome::Turn(TurnResult::Handoff { target, payload }) => Advance::Complete {
                output: serde_json::json!({ "handoff": { "target": target, "payload": payload } }),
            },
            TurnOutcome::Paused { pending } => {
                let request = pending
                    .first()
                    .map(|p| ApprovalRequest {
                        tool_call_id: p.tool_call.id.clone(),
                        summary: format!("Approve tool call: {}", p.tool_call.name),
                        risk: format!("{:?}", p.risk),
                    })
                    .unwrap_or_else(|| ApprovalRequest {
                        tool_call_id: String::new(),
                        summary: "approval required".into(),
                        risk: "unknown".into(),
                    });
                Advance::WaitForApproval { request }
            }
        })
    }
}

/// A Sweet [`AgentIo`] that mirrors streaming tool events into the Suxel event
/// log. Content deltas are not journaled (they belong to the chat transcript
/// Sweet's session already keeps); orchestration-relevant tool events are.
pub struct DurableIo {
    backend: Arc<dyn Backend>,
    run_id: RunId,
    /// Approval decisions delivered for this run, keyed by tool-call id. A call
    /// without a decision is deferred (pausing the turn).
    approvals: HashMap<String, ApprovalDecision>,
}

impl DurableIo {
    /// Construct a durable IO for `run_id` with no pre-delivered approvals (so
    /// any tool needing approval will pause the run).
    pub fn new(backend: Arc<dyn Backend>, run_id: RunId) -> Self {
        Self::with_approvals(backend, run_id, HashMap::new())
    }

    /// Construct a durable IO with approval decisions already delivered (keyed by
    /// tool-call id) — used on resume, so the previously-deferred calls now
    /// resolve instead of pausing again. A driver builds the map from
    /// [`RunContext::take_approvals`](suxel_core::driver::RunContext::take_approvals).
    pub fn with_approvals(
        backend: Arc<dyn Backend>,
        run_id: RunId,
        approvals: HashMap<String, ApprovalDecision>,
    ) -> Self {
        Self {
            backend,
            run_id,
            approvals,
        }
    }
}

#[async_trait]
impl AgentIo for DurableIo {
    async fn read_input(&mut self) -> sweet_core::Result<Option<String>> {
        // The driver injects input directly via `step_stream`; the IO never reads.
        Ok(None)
    }

    async fn write_reply(
        &mut self,
        _message: &Message,
        _session: &dyn Session,
    ) -> sweet_core::Result<()> {
        Ok(())
    }

    async fn on_tool_call(&mut self, call: &ToolCall) -> sweet_core::Result<()> {
        // Journaling failures must never break the turn; log-and-continue.
        let _ = self
            .backend
            .append(
                self.run_id,
                EventKind::ToolCallRecorded,
                serde_json::json!({ "id": call.id, "name": call.name }),
            )
            .await;
        Ok(())
    }

    async fn on_tool_result(&mut self, call: &ToolCall, result: &str) -> sweet_core::Result<()> {
        let preview: String = result.chars().take(500).collect();
        let _ = self
            .backend
            .append(
                self.run_id,
                EventKind::ToolCallRecorded,
                serde_json::json!({ "id": call.id, "name": call.name, "result_preview": preview }),
            )
            .await;
        Ok(())
    }

    async fn on_tool_approval(
        &mut self,
        call: &ToolCall,
        _risk: ToolRisk,
    ) -> sweet_core::Result<SweetApproval> {
        // A delivered decision resolves the call; otherwise defer to pause the
        // run so a human (or policy) can decide durably.
        Ok(match self.approvals.get(&call.id) {
            Some(ApprovalDecision::Allow) => SweetApproval::Allow,
            Some(ApprovalDecision::AllowAlways) => SweetApproval::AllowSession,
            Some(ApprovalDecision::Deny) => SweetApproval::Deny,
            None => SweetApproval::Defer,
        })
    }
}

/// Decorate a Sweet [`ToolSpec`] so its handler memoizes its result in the
/// durable-step journal: a re-run (worker crash + re-claim, or in-turn retry)
/// with the same arguments returns the saved result instead of re-executing the
/// side effect. Only successful results are journaled (errors stay retryable).
///
/// The memoization key is `(run_id, tool_name, args)`, so **two calls with
/// identical arguments in one run are treated as the same step** — the second
/// returns the first's saved result and its side effect is *not* re-run. Only
/// wrap tools whose repeated identical-args calls are safe to collapse this way
/// (pure reads, idempotent effects). Do not wrap a tool whose two identical
/// calls are meant to have distinct effects (e.g. "append the same line twice").
pub fn wrap_tool(spec: ToolSpec, run_id: RunId, backend: Arc<dyn Backend>) -> ToolSpec {
    let handler = Arc::new(DurableToolHandler {
        inner: spec.handler.clone(),
        tool_name: spec.name.clone(),
        run_id,
        backend,
    });
    ToolSpec {
        name: spec.name,
        description: spec.description,
        parameters_schema: spec.parameters_schema,
        handler,
        risk: spec.risk,
    }
}

struct DurableToolHandler {
    inner: Arc<dyn ToolHandler>,
    tool_name: String,
    run_id: RunId,
    backend: Arc<dyn Backend>,
}

impl DurableToolHandler {
    fn step_key(&self, args: &serde_json::Value) -> String {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        args.to_string().hash(&mut hasher);
        format!("tool:{}:{:x}", self.tool_name, hasher.finish())
    }
}

#[async_trait]
impl ToolHandler for DurableToolHandler {
    async fn call(&self, args: serde_json::Value) -> std::result::Result<String, ToolError> {
        Ok(self.call_rich(args).await?.text_content())
    }

    async fn call_rich(
        &self,
        args: serde_json::Value,
    ) -> std::result::Result<ToolOutput, ToolError> {
        let key = self.step_key(&args);

        // Memoized? Return the saved blocks without re-running the side effect.
        if let Ok(Some(record)) = self.backend.lookup_step(self.run_id, &key).await {
            if record.status == StepStatus::Completed {
                if let Some(out) = record.output {
                    if let Ok(blocks) = serde_json::from_value::<Vec<ContentBlock>>(out) {
                        return Ok(ToolOutput { blocks });
                    }
                }
            }
        }

        let output = self.inner.call_rich(args).await?;

        if let Ok(blocks_json) = serde_json::to_value(&output.blocks) {
            let _ = self
                .backend
                .record_step(&StepRecord {
                    run_id: self.run_id,
                    key: StepKey(key),
                    status: StepStatus::Completed,
                    output: Some(blocks_json),
                    error: None,
                    attempts: 1,
                    updated_at: chrono::Utc::now(),
                })
                .await;
        }
        Ok(output)
    }
}
