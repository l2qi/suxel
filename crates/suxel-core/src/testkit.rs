// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Reusable mock drivers and conformance scenarios, behind the `testkit` feature.
//!
//! Every storage backend should satisfy the same engine semantics. Rather than
//! duplicate tests per backend, each scenario here takes an `Arc<dyn Backend>`
//! and asserts a behavior; `suxel-core`'s own tests run them against the
//! in-memory backend, and `suxel-store-*` crates run the identical set.

use crate::driver::{Advance, AgentDriver, ApprovalRequest, ChildSpec, RunContext};
use crate::resource::ResourceKind;
use crate::run::{JoinMode, RunStatus};
use crate::signal::{ApprovalDecision, ApprovalResolution};
use crate::store::Backend;
use crate::{Budget, Engine, EventKind, RetryPolicy, RunSpec};
use async_trait::async_trait;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const W: &str = "testkit-worker";

// --- drivers ----------------------------------------------------------------

/// Completes immediately, echoing its goal.
pub struct EchoDriver;
#[async_trait]
impl AgentDriver for EchoDriver {
    async fn advance(&self, ctx: &mut RunContext) -> crate::Result<Advance> {
        Ok(Advance::Complete {
            output: json!({ "echo": ctx.goal() }),
        })
    }
}

/// Runs one durable side effect across two advances; it must run exactly once.
pub struct SideEffectDriver {
    advances: Arc<AtomicU64>,
    effect: Arc<AtomicU64>,
}
#[async_trait]
impl AgentDriver for SideEffectDriver {
    async fn advance(&self, ctx: &mut RunContext) -> crate::Result<Advance> {
        let n = self.advances.fetch_add(1, Ordering::SeqCst) + 1;
        let effect = self.effect.clone();
        let value: u64 = ctx
            .durable_step("side_effect", RetryPolicy::none(), move || {
                let effect = effect.clone();
                async move { Ok(effect.fetch_add(1, Ordering::SeqCst) + 1) }
            })
            .await?;
        if n == 1 {
            Ok(Advance::Continue)
        } else {
            Ok(Advance::Complete {
                output: json!({ "value": value }),
            })
        }
    }
}

/// A flaky step that fails twice then succeeds.
pub struct RetryDriver {
    attempts: Arc<AtomicU64>,
}
#[async_trait]
impl AgentDriver for RetryDriver {
    async fn advance(&self, ctx: &mut RunContext) -> crate::Result<Advance> {
        let attempts = self.attempts.clone();
        let policy = RetryPolicy {
            max_attempts: 3,
            base_delay: std::time::Duration::from_millis(1),
            max_delay: std::time::Duration::from_millis(1),
        };
        let v: u64 = ctx
            .durable_step("flaky", policy, move || {
                let attempts = attempts.clone();
                async move {
                    let a = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if a < 3 {
                        Err(format!("transient {a}"))
                    } else {
                        Ok(a)
                    }
                }
            })
            .await?;
        Ok(Advance::Complete {
            output: json!({ "attempts": v }),
        })
    }
}

/// Parks until a `"go"` signal, then completes with its payload.
pub struct SignalDriver;
#[async_trait]
impl AgentDriver for SignalDriver {
    async fn advance(&self, ctx: &mut RunContext) -> crate::Result<Advance> {
        if let Some(payload) = ctx.take_signal("go").await? {
            Ok(Advance::Complete { output: payload })
        } else {
            Ok(Advance::WaitForSignal {
                name: "go".into(),
                timeout: None,
            })
        }
    }
}

/// Parks for approval, then completes (allow) or fails (deny).
pub struct ApprovalDriver;
#[async_trait]
impl AgentDriver for ApprovalDriver {
    async fn advance(&self, ctx: &mut RunContext) -> crate::Result<Advance> {
        match ctx.take_approvals().await?.first() {
            Some(res) => match res.decision {
                ApprovalDecision::Allow | ApprovalDecision::AllowAlways => Ok(Advance::Complete {
                    output: json!({ "approved": true }),
                }),
                ApprovalDecision::Deny => Ok(Advance::Fail {
                    error: "denied".into(),
                    retry: false,
                }),
            },
            None => Ok(Advance::WaitForApproval {
                request: ApprovalRequest {
                    tool_call_id: "call-1".into(),
                    summary: "send the email".into(),
                    risk: "dangerous".into(),
                },
            }),
        }
    }
}

/// Spawns children with a join mode, then aggregates.
pub struct ParentDriver {
    join: JoinMode,
    specs: Vec<ChildSpec>,
}
#[async_trait]
impl AgentDriver for ParentDriver {
    async fn advance(&self, ctx: &mut RunContext) -> crate::Result<Advance> {
        let kids = ctx.children().await?;
        if kids.is_empty() {
            Ok(Advance::SpawnChildren {
                specs: self.specs.clone(),
                join: self.join,
            })
        } else {
            Ok(Advance::Complete {
                output: json!({ "children": kids.len() }),
            })
        }
    }
}

/// Completes "fast" or parks forever when input mode is "slow".
pub struct WorkerDriver;
#[async_trait]
impl AgentDriver for WorkerDriver {
    async fn advance(&self, ctx: &mut RunContext) -> crate::Result<Advance> {
        let mode = ctx
            .input()
            .and_then(|v| v.get("mode"))
            .and_then(|m| m.as_str())
            .unwrap_or("fast")
            .to_string();
        if mode == "slow" {
            Ok(Advance::WaitForSignal {
                name: "never".into(),
                timeout: None,
            })
        } else {
            Ok(Advance::Complete {
                output: json!({ "mode": mode }),
            })
        }
    }
}

/// Charges a tool call every advance and loops; only a budget can stop it.
pub struct BudgetBurnDriver;
#[async_trait]
impl AgentDriver for BudgetBurnDriver {
    async fn advance(&self, ctx: &mut RunContext) -> crate::Result<Advance> {
        ctx.charge_tool_call();
        Ok(Advance::Continue)
    }
}

/// Leases a resource and produces an artifact, then completes.
pub struct ResourceArtifactDriver;
#[async_trait]
impl AgentDriver for ResourceArtifactDriver {
    async fn advance(&self, ctx: &mut RunContext) -> crate::Result<Advance> {
        ctx.lease_resource(
            ResourceKind::CodeSandbox,
            json!({ "sandbox_id": "sb-1" }),
            None,
        )
        .await?;
        ctx.put_artifact("report", "mem://report.md", json!({ "bytes": 12 }))
            .await?;
        Ok(Advance::Complete {
            output: json!({ "done": true }),
        })
    }
}

// --- scenarios --------------------------------------------------------------

/// A simple run completes and logs `RunCreated` + `RunCompleted` events.
pub async fn assert_completes_simple(backend: Arc<dyn Backend>) {
    let engine = Engine::new(backend).with_driver("echo", Arc::new(EchoDriver));
    let id = engine
        .create_run(RunSpec {
            agent_type: "echo".into(),
            goal: "hi".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();

    let run = engine.get_run(id).await.unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    assert_eq!(run.output.unwrap()["echo"], "hi");
    let kinds: Vec<_> = engine
        .backend()
        .read(id, 0)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.kind)
        .collect();
    assert!(kinds.contains(&EventKind::RunCreated));
    assert!(kinds.contains(&EventKind::RunCompleted));
}

/// A durable step's side effect runs exactly once across two advances.
pub async fn assert_durable_step_once(backend: Arc<dyn Backend>) {
    let effect = Arc::new(AtomicU64::new(0));
    let driver = Arc::new(SideEffectDriver {
        advances: Arc::new(AtomicU64::new(0)),
        effect: effect.clone(),
    });
    let engine = Engine::new(backend).with_driver("fx", driver);
    let id = engine
        .create_run(RunSpec {
            agent_type: "fx".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();

    let run = engine.get_run(id).await.unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    assert_eq!(
        effect.load(Ordering::SeqCst),
        1,
        "side effect ran more than once"
    );
    assert_eq!(run.output.unwrap()["value"], 1);
}

/// A flaky step retries until it succeeds.
pub async fn assert_retries(backend: Arc<dyn Backend>) {
    let attempts = Arc::new(AtomicU64::new(0));
    let engine = Engine::new(backend).with_driver(
        "retry",
        Arc::new(RetryDriver {
            attempts: attempts.clone(),
        }),
    );
    let id = engine
        .create_run(RunSpec {
            agent_type: "retry".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();

    assert_eq!(
        engine.get_run(id).await.unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

/// A run parks on a signal and resumes when it arrives.
pub async fn assert_signal_park_resume(backend: Arc<dyn Backend>) {
    let engine = Engine::new(backend).with_driver("sig", Arc::new(SignalDriver));
    let id = engine
        .create_run(RunSpec {
            agent_type: "sig".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();
    assert_eq!(
        engine.get_run(id).await.unwrap().status,
        RunStatus::WaitingSignal
    );

    engine
        .signal(id, "go", json!({ "from": "test" }))
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();
    let run = engine.get_run(id).await.unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    assert_eq!(run.output.unwrap()["from"], "test");
}

/// A run parks for approval; allow resumes it to completion.
pub async fn assert_approval_park_resume(backend: Arc<dyn Backend>) {
    let engine = Engine::new(backend).with_driver("appr", Arc::new(ApprovalDriver));
    let id = engine
        .create_run(RunSpec {
            agent_type: "appr".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();
    assert_eq!(
        engine.get_run(id).await.unwrap().status,
        RunStatus::WaitingApproval
    );

    engine
        .approve(
            id,
            ApprovalResolution {
                tool_call_id: "call-1".into(),
                decision: ApprovalDecision::Allow,
            },
        )
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();
    assert_eq!(
        engine.get_run(id).await.unwrap().status,
        RunStatus::Completed
    );
}

/// Denying an approval fails the run.
pub async fn assert_denied_approval_fails(backend: Arc<dyn Backend>) {
    let engine = Engine::new(backend).with_driver("appr", Arc::new(ApprovalDriver));
    let id = engine
        .create_run(RunSpec {
            agent_type: "appr".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();
    engine
        .approve(
            id,
            ApprovalResolution {
                tool_call_id: "call-1".into(),
                decision: ApprovalDecision::Deny,
            },
        )
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();
    assert_eq!(engine.get_run(id).await.unwrap().status, RunStatus::Failed);
}

/// A parent spawns children and joins on all of them completing.
pub async fn assert_join_all(backend: Arc<dyn Backend>) {
    let specs: Vec<ChildSpec> = (0..3)
        .map(|i| ChildSpec {
            agent_type: "child".into(),
            goal: format!("task {i}"),
            ..Default::default()
        })
        .collect();
    let engine = Engine::new(backend)
        .with_driver(
            "parent",
            Arc::new(ParentDriver {
                join: JoinMode::All,
                specs,
            }),
        )
        .with_driver("child", Arc::new(EchoDriver));
    let id = engine
        .create_run(RunSpec {
            agent_type: "parent".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();

    let run = engine.get_run(id).await.unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    assert_eq!(run.output.unwrap()["children"], 3);
    let kids = engine.backend().list_children(id).await.unwrap();
    assert_eq!(kids.len(), 3);
    assert!(kids.iter().all(|k| k.status == RunStatus::Completed));
}

/// An `Any` join wakes on the first child and cancels the rest.
pub async fn assert_join_any_cancels(backend: Arc<dyn Backend>) {
    let specs = vec![
        ChildSpec {
            agent_type: "worker".into(),
            goal: "fast".into(),
            input: Some(json!({ "mode": "fast" })),
            ..Default::default()
        },
        ChildSpec {
            agent_type: "worker".into(),
            goal: "slow".into(),
            input: Some(json!({ "mode": "slow" })),
            ..Default::default()
        },
    ];
    let engine = Engine::new(backend)
        .with_driver(
            "parent",
            Arc::new(ParentDriver {
                join: JoinMode::Any,
                specs,
            }),
        )
        .with_driver("worker", Arc::new(WorkerDriver));
    let id = engine
        .create_run(RunSpec {
            agent_type: "parent".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();

    assert_eq!(
        engine.get_run(id).await.unwrap().status,
        RunStatus::Completed
    );
    let kids = engine.backend().list_children(id).await.unwrap();
    let fast = kids.iter().find(|k| k.goal == "fast").unwrap();
    let slow = kids.iter().find(|k| k.goal == "slow").unwrap();
    assert_eq!(fast.status, RunStatus::Completed);
    assert_eq!(slow.status, RunStatus::Cancelled);
}

/// Breaching a budget fails the run.
pub async fn assert_budget_breach(backend: Arc<dyn Backend>) {
    let engine = Engine::new(backend).with_driver("burn", Arc::new(BudgetBurnDriver));
    let id = engine
        .create_run(RunSpec {
            agent_type: "burn".into(),
            budget: Budget {
                max_tool_calls: Some(0),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();
    let run = engine.get_run(id).await.unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    assert!(run.error.unwrap().contains("budget"));
}

/// A run can lease a resource and produce an artifact durably.
pub async fn assert_resource_and_artifact(backend: Arc<dyn Backend>) {
    let engine = Engine::new(backend).with_driver("ra", Arc::new(ResourceArtifactDriver));
    let id = engine
        .create_run(RunSpec {
            agent_type: "ra".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle(W).await.unwrap();

    assert_eq!(
        engine.get_run(id).await.unwrap().status,
        RunStatus::Completed
    );
    let resources = engine.backend().list_resources(id).await.unwrap();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].metadata["sandbox_id"], "sb-1");
    let artifacts = engine.backend().list_artifacts(id).await.unwrap();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].kind, "report");
}

/// Signals are consumed one at a time by name: taking one leaves every other
/// delivered signal — including later ones of the same name — intact. A run must
/// never lose a signal it was delivered but did not take this turn.
pub async fn assert_signals_selective_consume(backend: Arc<dyn Backend>) {
    let engine = Engine::new(backend.clone());
    let id = engine
        .create_run(RunSpec {
            agent_type: "x".into(),
            ..Default::default()
        })
        .await
        .unwrap();

    backend.deliver(id, "a", json!(1)).await.unwrap();
    backend.deliver(id, "b", json!(2)).await.unwrap();
    backend.deliver(id, "a", json!(3)).await.unwrap();

    // Take one "a": get the earliest, and "b" plus the later "a" survive.
    let first = backend
        .take_signal(id, "a")
        .await
        .unwrap()
        .expect("first a");
    assert_eq!(first.payload, json!(1));
    assert!(
        backend.has_unconsumed(id, "b").await.unwrap(),
        "an untaken signal must not be consumed"
    );
    let second = backend
        .take_signal(id, "a")
        .await
        .unwrap()
        .expect("second a");
    assert_eq!(second.payload, json!(3));
    assert!(backend.take_signal(id, "a").await.unwrap().is_none());
    let b = backend.take_signal(id, "b").await.unwrap().expect("b");
    assert_eq!(b.payload, json!(2));
}

/// Run the full conformance suite against `backend` (a fresh, empty backend).
/// Each scenario creates its own runs, so they can share one backend.
pub async fn run_all(make_backend: impl Fn() -> Arc<dyn Backend>) {
    assert_completes_simple(make_backend()).await;
    assert_durable_step_once(make_backend()).await;
    assert_retries(make_backend()).await;
    assert_signal_park_resume(make_backend()).await;
    assert_signals_selective_consume(make_backend()).await;
    assert_approval_park_resume(make_backend()).await;
    assert_denied_approval_fails(make_backend()).await;
    assert_join_all(make_backend()).await;
    assert_join_any_cancels(make_backend()).await;
    assert_budget_breach(make_backend()).await;
    assert_resource_and_artifact(make_backend()).await;
}
