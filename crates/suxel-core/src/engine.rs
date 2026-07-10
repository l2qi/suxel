// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! The durable execution engine: registers drivers, creates runs, and runs the
//! worker loop that claims runs, advances them through their drivers, and makes
//! every transition durable.

use crate::driver::{Advance, AgentDriver, RunContext};
use crate::error::{Error, Result};
use crate::event::EventKind;
use crate::ids::RunId;
use crate::run::{JoinMode, Run, RunSpec, RunStatus, Wait};
use crate::signal::{ApprovalResolution, APPROVAL_SIGNAL};
use crate::store::Backend;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// The durable execution engine.
pub struct Engine {
    backend: Arc<dyn Backend>,
    drivers: HashMap<String, Arc<dyn AgentDriver>>,
    lease_ttl: Duration,
    poll_interval: Duration,
}

impl Engine {
    /// Create an engine over `backend` with no drivers yet.
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Engine {
            backend,
            drivers: HashMap::new(),
            lease_ttl: Duration::from_secs(30),
            poll_interval: Duration::from_millis(50),
        }
    }

    /// Register a driver for an `agent_type` (builder style).
    pub fn with_driver(
        mut self,
        agent_type: impl Into<String>,
        driver: Arc<dyn AgentDriver>,
    ) -> Self {
        self.drivers.insert(agent_type.into(), driver);
        self
    }

    /// Set the claim lease TTL.
    pub fn with_lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }

    /// Set the idle poll interval used by [`Engine::run_worker`].
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Access the backend (for reads outside the engine, e.g. event streaming).
    pub fn backend(&self) -> &Arc<dyn Backend> {
        &self.backend
    }

    // ---- public API ----------------------------------------------------------

    /// Create and enqueue a new run.
    pub async fn create_run(&self, spec: RunSpec) -> Result<RunId> {
        let run = Run::from_spec(spec, self.backend.now());
        self.backend.create_run(&run).await?;
        self.backend
            .append(
                run.id,
                EventKind::RunCreated,
                serde_json::json!({ "agent_type": run.agent_type, "goal": run.goal }),
            )
            .await?;
        self.backend.enqueue(run.id, None).await?;
        Ok(run.id)
    }

    /// Fetch a run.
    pub async fn get_run(&self, id: RunId) -> Result<Run> {
        self.backend.get_run(id).await
    }

    /// Deliver a named signal; wakes the run if it is parked on that name.
    pub async fn signal(
        &self,
        run_id: RunId,
        name: &str,
        payload: serde_json::Value,
    ) -> Result<()> {
        self.backend.deliver(run_id, name, payload).await?;
        self.backend
            .append(
                run_id,
                EventKind::SignalReceived,
                serde_json::json!({ "name": name }),
            )
            .await?;
        let mut run = self.backend.get_run(run_id).await?;
        if run.status == RunStatus::WaitingSignal {
            let matches = matches!(&run.waiting, Some(Wait::Signal { name: n }) if n == name);
            if matches {
                self.wake(&mut run).await?;
            }
        }
        Ok(())
    }

    /// Deliver an approval decision; resumes a run parked on approval.
    pub async fn approve(&self, run_id: RunId, resolution: ApprovalResolution) -> Result<()> {
        self.backend
            .deliver(run_id, APPROVAL_SIGNAL, serde_json::to_value(&resolution)?)
            .await?;
        self.backend
            .append(
                run_id,
                EventKind::ApprovalResolved,
                serde_json::to_value(&resolution)?,
            )
            .await?;
        let mut run = self.backend.get_run(run_id).await?;
        if run.status == RunStatus::WaitingApproval {
            self.wake(&mut run).await?;
        }
        Ok(())
    }

    /// Cancel a run and its non-terminal descendants.
    pub async fn cancel(&self, run_id: RunId) -> Result<()> {
        let top = match self.backend.get_run(run_id).await {
            Ok(r) => r,
            Err(Error::RunNotFound(_)) => return Ok(()), // nothing to cancel
            Err(e) => return Err(e), // a real storage error must not read as success
        };
        let now = self.backend.now();
        let mut stack = vec![run_id];
        while let Some(id) = stack.pop() {
            let mut r = match self.backend.get_run(id).await {
                Ok(r) => r,
                Err(Error::RunNotFound(_)) => continue, // child vanished; skip it
                Err(e) => return Err(e),                // don't silently abort the cascade
            };
            if r.status.is_terminal() {
                continue;
            }
            r.status = RunStatus::Cancelled;
            r.waiting = None;
            r.updated_at = now;
            self.backend.update_run(&r).await?;
            self.backend
                .append(id, EventKind::RunCancelled, serde_json::json!({}))
                .await?;
            for c in self.backend.list_children(id).await? {
                if !c.status.is_terminal() {
                    stack.push(c.id);
                }
            }
        }
        if let Some(pid) = top.parent_id {
            self.on_child_terminal(pid).await?;
        }
        Ok(())
    }

    // ---- worker loop ---------------------------------------------------------

    /// Claim and process one ready run. Returns `Ok(false)` if nothing was ready,
    /// `Ok(true)` after one is processed. On a processing error the claim is left
    /// leased (so it redelivers on lease expiry) and the error is **returned**,
    /// not acked-and-swallowed — a caller must not read a failure as "done".
    pub async fn tick(&self, worker_id: &str) -> Result<bool> {
        let Some(lease) = self.backend.claim(worker_id, self.lease_ttl).await? else {
            return Ok(false);
        };
        match self.process(lease.item).await {
            Ok(()) => {
                self.backend.ack(&lease).await?;
                Ok(true)
            }
            // Do NOT ack: leaving the entry leased lets it redeliver once the lease
            // expires — the same path a crashed worker takes — so the run is retried
            // rather than stranded. Return the error so it surfaces (`run_worker`
            // logs it and moves on; `run_until_idle` propagates it) instead of being
            // swallowed while the run sits parked mid-lease.
            Err(e) => Err(e),
        }
    }

    /// Process currently-ready runs until none remain, then return `Ok(())`.
    /// (Test/embedded helper.) Returns `Err` if a run's processing fails on a
    /// backend error: that run is left leased and redelivers on lease expiry, so a
    /// caller can retry once the lease clears — the failure is surfaced, never
    /// silently reported as idle.
    pub async fn run_until_idle(&self, worker_id: &str) -> Result<()> {
        while self.tick(worker_id).await? {}
        Ok(())
    }

    /// Run the worker loop forever.
    pub async fn run_worker(&self, worker_id: &str) {
        loop {
            match self.tick(worker_id).await {
                Ok(true) => {}
                Ok(false) => tokio::time::sleep(self.poll_interval).await,
                Err(e) => {
                    tracing::error!(worker = worker_id, error = %e, "worker tick failed");
                    tokio::time::sleep(self.poll_interval).await;
                }
            }
        }
    }

    // ---- internals -----------------------------------------------------------

    async fn process(&self, run_id: RunId) -> Result<()> {
        // Cheap pre-check: skip a stale queue entry for an already-finished run
        // without taking the processing lease.
        if self.backend.get_run(run_id).await?.status.is_terminal() {
            return Ok(());
        }
        // Fence the run: the queue lease guards a row, not the run, so a second
        // ready entry for the same run could otherwise drive a concurrent advance
        // under another worker. If the lease is already held, drop this duplicate
        // claim (the caller acks the queue entry).
        if !self.backend.try_acquire_run(run_id, self.lease_ttl).await? {
            return Ok(());
        }
        let result = self.advance_once(run_id).await;
        // Release the fence so the next enqueue/claim can advance the run; a crash
        // mid-advance leaves it to expire via the lease TTL instead.
        let _ = self.backend.release_run(run_id).await;
        result
    }

    async fn advance_once(&self, run_id: RunId) -> Result<()> {
        // Re-read under the fence: the run may have reached a terminal state
        // between the pre-check and acquiring the lease.
        let mut run = self.backend.get_run(run_id).await?;
        if run.status.is_terminal() {
            return Ok(());
        }
        let Some(driver) = self.drivers.get(&run.agent_type).cloned() else {
            let msg = format!("no driver for agent_type {:?}", run.agent_type);
            return self.finish_failed(run, msg).await;
        };

        run.status = RunStatus::Running;
        run.updated_at = self.backend.now();
        self.backend.update_run(&run).await?;

        let mut ctx = RunContext::new(self.backend.clone(), run.clone());
        let advanced = driver.advance(&mut ctx).await;
        run.usage.add(&ctx.into_usage_delta());

        match advanced {
            Err(e) => self.finish_failed(run, e.to_string()).await,
            Ok(advance) => {
                if let Err(reason) = run.budget.check(&run.usage) {
                    return self
                        .finish_failed(run, format!("budget exceeded: {reason}"))
                        .await;
                }
                self.apply_advance(run, advance).await
            }
        }
    }

    async fn apply_advance(&self, mut run: Run, advance: Advance) -> Result<()> {
        let now = self.backend.now();
        match advance {
            Advance::Continue => {
                run.status = RunStatus::Pending;
                run.waiting = None;
                run.updated_at = now;
                self.backend.update_run(&run).await?;
                self.backend.enqueue(run.id, None).await?;
                Ok(())
            }
            Advance::Complete { output } => {
                run.status = RunStatus::Completed;
                run.output = Some(output);
                run.waiting = None;
                run.updated_at = now;
                self.backend.update_run(&run).await?;
                self.backend
                    .append(run.id, EventKind::RunCompleted, serde_json::json!({}))
                    .await?;
                self.notify_parent(&run).await
            }
            Advance::Fail { error, retry } => {
                if retry {
                    run.status = RunStatus::Pending;
                    run.waiting = None;
                    run.updated_at = now;
                    self.backend.update_run(&run).await?;
                    self.backend.enqueue(run.id, None).await?;
                    Ok(())
                } else {
                    self.finish_failed(run, error).await
                }
            }
            Advance::WaitForApproval { request } => {
                if self.backend.has_unconsumed(run.id, APPROVAL_SIGNAL).await? {
                    // An approval was delivered while we were processing (before
                    // this park committed); don't park — reprocess so the driver
                    // consumes it. Mirrors the WaitForSignal pre-park check and
                    // closes the deliver-before-park race that would hang the run.
                    run.status = RunStatus::Pending;
                    run.waiting = None;
                    run.updated_at = now;
                    self.backend.update_run(&run).await?;
                    self.backend.enqueue(run.id, None).await?;
                    return Ok(());
                }
                run.status = RunStatus::WaitingApproval;
                run.waiting = Some(Wait::Approval);
                run.updated_at = now;
                self.backend.update_run(&run).await?;
                self.backend
                    .append(
                        run.id,
                        EventKind::ApprovalRequested,
                        serde_json::to_value(&request)?,
                    )
                    .await?;
                Ok(())
            }
            Advance::WaitForSignal { name, timeout } => {
                if self.backend.has_unconsumed(run.id, &name).await? {
                    // The signal already arrived; don't park.
                    run.status = RunStatus::Pending;
                    run.waiting = None;
                    run.updated_at = now;
                    self.backend.update_run(&run).await?;
                    self.backend.enqueue(run.id, None).await?;
                    return Ok(());
                }
                run.status = RunStatus::WaitingSignal;
                run.waiting = Some(Wait::Signal { name });
                run.updated_at = now;
                self.backend.update_run(&run).await?;
                if let Some(t) = timeout {
                    let ready = now
                        + chrono::Duration::from_std(t)
                            .unwrap_or_else(|_| chrono::Duration::seconds(1));
                    self.backend.enqueue(run.id, Some(ready)).await?;
                }
                Ok(())
            }
            Advance::Sleep { until } => {
                run.status = RunStatus::WaitingTimer;
                run.waiting = Some(Wait::Timer);
                run.updated_at = now;
                self.backend.update_run(&run).await?;
                self.backend
                    .append(
                        run.id,
                        EventKind::TimerSet,
                        serde_json::json!({ "until": until }),
                    )
                    .await?;
                self.backend.enqueue(run.id, Some(until)).await?;
                Ok(())
            }
            Advance::SpawnChildren { specs, join } => {
                run.usage.children += specs.len() as u64;
                if let Err(reason) = run.budget.check(&run.usage) {
                    return self
                        .finish_failed(run, format!("budget exceeded: {reason}"))
                        .await;
                }
                let mut child_ids = Vec::with_capacity(specs.len());
                for spec in specs {
                    let child = Run::from_spec(
                        RunSpec {
                            agent_type: spec.agent_type,
                            goal: spec.goal,
                            input: spec.input,
                            session_ref: spec.session_ref,
                            budget: spec.budget.unwrap_or_else(|| run.budget.clone()),
                            parent_id: Some(run.id),
                        },
                        now,
                    );
                    self.backend.create_run(&child).await?;
                    self.backend
                        .append(
                            child.id,
                            EventKind::RunCreated,
                            serde_json::json!({ "agent_type": child.agent_type, "goal": child.goal }),
                        )
                        .await?;
                    self.backend.enqueue(child.id, None).await?;
                    child_ids.push(child.id);
                }
                run.status = RunStatus::WaitingSignal;
                run.waiting = Some(Wait::ChildJoin { mode: join });
                run.updated_at = now;
                self.backend.update_run(&run).await?;
                self.backend
                    .append(
                        run.id,
                        EventKind::ChildRunSpawned,
                        serde_json::json!({ "children": child_ids }),
                    )
                    .await?;
                Ok(())
            }
        }
    }

    async fn finish_failed(&self, mut run: Run, error: String) -> Result<()> {
        run.status = RunStatus::Failed;
        run.error = Some(error.clone());
        run.waiting = None;
        run.updated_at = self.backend.now();
        self.backend.update_run(&run).await?;
        self.backend
            .append(
                run.id,
                EventKind::RunFailed,
                serde_json::json!({ "error": error }),
            )
            .await?;
        self.notify_parent(&run).await
    }

    async fn wake(&self, run: &mut Run) -> Result<()> {
        run.status = RunStatus::Pending;
        run.waiting = None;
        run.updated_at = self.backend.now();
        self.backend.update_run(run).await?;
        self.backend.enqueue(run.id, None).await
    }

    async fn notify_parent(&self, child: &Run) -> Result<()> {
        if let Some(pid) = child.parent_id {
            self.on_child_terminal(pid).await?;
        }
        Ok(())
    }

    /// A child reached a terminal state; evaluate the parent's join and wake it
    /// if satisfied. Boxed because it can mutually recurse with `cancel`.
    fn on_child_terminal(
        &self,
        parent_id: RunId,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut parent = match self.backend.get_run(parent_id).await {
                Ok(p) => p,
                Err(_) => return Ok(()),
            };
            let Some(Wait::ChildJoin { mode }) = parent.waiting.clone() else {
                return Ok(()); // not (or no longer) joining
            };
            let children = self.backend.list_children(parent_id).await?;
            let all_terminal = children.iter().all(|c| c.status.is_terminal());
            let any_terminal = children.iter().any(|c| c.status.is_terminal());
            let succeeded = children
                .iter()
                .filter(|c| c.status == RunStatus::Completed)
                .count() as u32;
            let satisfied = match mode {
                JoinMode::All => all_terminal,
                JoinMode::Any => any_terminal,
                JoinMode::Quorum(n) => succeeded >= n || all_terminal,
            };
            self.backend
                .append(
                    parent_id,
                    EventKind::ChildRunFinished,
                    serde_json::json!({ "satisfied": satisfied }),
                )
                .await?;
            if satisfied {
                self.wake(&mut parent).await?;
                if let JoinMode::Any = mode {
                    for c in &children {
                        if !c.status.is_terminal() {
                            self.cancel(c.id).await?;
                        }
                    }
                }
            }
            Ok(())
        })
    }
}
