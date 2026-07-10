// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! A backend error during `process` is surfaced by `tick`/`run_until_idle` (not
//! swallowed while the run sits parked mid-lease), and the run redelivers on
//! lease expiry rather than being stranded.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use suxel_core::artifact::Artifact;
use suxel_core::driver::{Advance, AgentDriver, RunContext};
use suxel_core::error::{Error, Result};
use suxel_core::event::{Event, EventKind};
use suxel_core::ids::{ResourceId, RunId};
use suxel_core::resource::Resource;
use suxel_core::run::Run;
use suxel_core::signal::Signal;
use suxel_core::step::StepRecord;
use suxel_core::store::{
    ArtifactStore, Clock, EventLog, Lease, Queue, ResourceStore, RunStore, SignalStore, StepStore,
};
use suxel_core::{Engine, InMemoryBackend, RunSpec, RunStatus};

/// Wraps the in-memory backend and fails `update_run` while `fail_update` is set,
/// forcing `process` to return a backend `Err` (as opposed to a driver error,
/// which the engine turns into a clean `finish_failed`).
struct FaultyBackend {
    inner: Arc<InMemoryBackend>,
    fail_update: AtomicBool,
}

#[async_trait]
impl RunStore for FaultyBackend {
    async fn create_run(&self, run: &Run) -> Result<()> {
        self.inner.create_run(run).await
    }
    async fn get_run(&self, id: RunId) -> Result<Run> {
        self.inner.get_run(id).await
    }
    async fn update_run(&self, run: &Run) -> Result<()> {
        if self.fail_update.load(Ordering::SeqCst) {
            return Err(Error::storage("injected update_run failure"));
        }
        self.inner.update_run(run).await
    }
    async fn list_children(&self, parent: RunId) -> Result<Vec<Run>> {
        self.inner.list_children(parent).await
    }
}

#[async_trait]
impl EventLog for FaultyBackend {
    async fn append(
        &self,
        run_id: RunId,
        kind: EventKind,
        payload: serde_json::Value,
    ) -> Result<u64> {
        self.inner.append(run_id, kind, payload).await
    }
    async fn read(&self, run_id: RunId, from_seq: u64) -> Result<Vec<Event>> {
        self.inner.read(run_id, from_seq).await
    }
}

#[async_trait]
impl StepStore for FaultyBackend {
    async fn lookup_step(&self, run_id: RunId, key: &str) -> Result<Option<StepRecord>> {
        self.inner.lookup_step(run_id, key).await
    }
    async fn record_step(&self, record: &StepRecord) -> Result<()> {
        self.inner.record_step(record).await
    }
}

#[async_trait]
impl Queue for FaultyBackend {
    async fn enqueue(&self, run_id: RunId, ready_at: Option<DateTime<Utc>>) -> Result<()> {
        self.inner.enqueue(run_id, ready_at).await
    }
    async fn claim(&self, worker_id: &str, lease_ttl: Duration) -> Result<Option<Lease<RunId>>> {
        self.inner.claim(worker_id, lease_ttl).await
    }
    async fn ack(&self, lease: &Lease<RunId>) -> Result<()> {
        self.inner.ack(lease).await
    }
}

#[async_trait]
impl SignalStore for FaultyBackend {
    async fn deliver(&self, run_id: RunId, name: &str, payload: serde_json::Value) -> Result<()> {
        self.inner.deliver(run_id, name, payload).await
    }
    async fn take_signal(&self, run_id: RunId, name: &str) -> Result<Option<Signal>> {
        self.inner.take_signal(run_id, name).await
    }
    async fn has_unconsumed(&self, run_id: RunId, name: &str) -> Result<bool> {
        self.inner.has_unconsumed(run_id, name).await
    }
}

#[async_trait]
impl ResourceStore for FaultyBackend {
    async fn lease_resource(&self, resource: &Resource) -> Result<()> {
        self.inner.lease_resource(resource).await
    }
    async fn heartbeat(&self, id: ResourceId, expires_at: DateTime<Utc>) -> Result<()> {
        self.inner.heartbeat(id, expires_at).await
    }
    async fn release_resource(&self, id: ResourceId) -> Result<()> {
        self.inner.release_resource(id).await
    }
    async fn list_resources(&self, run_id: RunId) -> Result<Vec<Resource>> {
        self.inner.list_resources(run_id).await
    }
}

#[async_trait]
impl ArtifactStore for FaultyBackend {
    async fn put_artifact(&self, artifact: &Artifact) -> Result<()> {
        self.inner.put_artifact(artifact).await
    }
    async fn list_artifacts(&self, run_id: RunId) -> Result<Vec<Artifact>> {
        self.inner.list_artifacts(run_id).await
    }
}

impl Clock for FaultyBackend {
    fn now(&self) -> DateTime<Utc> {
        self.inner.now()
    }
}

/// Completes immediately.
struct DoneDriver;
#[async_trait]
impl AgentDriver for DoneDriver {
    async fn advance(&self, _ctx: &mut RunContext) -> Result<Advance> {
        Ok(Advance::Complete {
            output: serde_json::json!({}),
        })
    }
}

#[tokio::test]
async fn process_error_surfaces_and_run_redelivers() {
    let inner = Arc::new(InMemoryBackend::new());
    let backend = Arc::new(FaultyBackend {
        inner: inner.clone(),
        fail_update: AtomicBool::new(false),
    });
    let engine = Engine::new(backend.clone())
        .with_driver("done", Arc::new(DoneDriver))
        .with_lease_ttl(Duration::from_millis(80));
    let id = engine
        .create_run(RunSpec {
            agent_type: "done".into(),
            ..Default::default()
        })
        .await
        .unwrap();

    // Fail the `update_run` that sets Running: `process` returns Err, so the run
    // must not complete and `run_until_idle` must surface the error (not idle).
    backend.fail_update.store(true, Ordering::SeqCst);
    assert!(
        engine.run_until_idle("w").await.is_err(),
        "a processing error must surface, not report idle"
    );
    assert_ne!(
        inner.get_run(id).await.unwrap().status,
        RunStatus::Completed
    );

    // The claim was left leased (not acked); once it expires the run redelivers,
    // and with the fault cleared it completes — it was retried, not stranded.
    backend.fail_update.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(130)).await;
    engine.run_until_idle("w").await.unwrap();
    assert_eq!(
        inner.get_run(id).await.unwrap().status,
        RunStatus::Completed
    );
}
