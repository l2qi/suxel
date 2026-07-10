// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage traits. A concrete backend (in-memory, SQLite, Postgres) implements
//! all of these; the [`Backend`] supertrait bundles them so the engine can hold
//! a single `Arc<dyn Backend>`.

use crate::artifact::Artifact;
use crate::error::Result;
use crate::event::{Event, EventKind};
use crate::ids::{ResourceId, RunId};
use crate::resource::Resource;
use crate::run::Run;
use crate::signal::Signal;
use crate::step::StepRecord;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;

/// Wall-clock source, injectable for deterministic tests.
pub trait Clock: Send + Sync {
    /// The current time.
    fn now(&self) -> DateTime<Utc>;
}

/// The default clock: the system wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A claimed item plus the lease that grants exclusive processing rights.
#[derive(Clone, Debug)]
pub struct Lease<T> {
    /// The leased item (a run id).
    pub item: T,
    /// Opaque lease handle; pass back to [`Queue::ack`].
    pub lease_id: uuid::Uuid,
    /// When the lease expires; after this another worker may re-claim.
    pub expires_at: DateTime<Utc>,
}

/// Run records.
#[async_trait]
pub trait RunStore: Send + Sync {
    /// Insert a new run.
    async fn create_run(&self, run: &Run) -> Result<()>;
    /// Fetch a run; `Err(RunNotFound)` if absent.
    async fn get_run(&self, id: RunId) -> Result<Run>;
    /// Overwrite a run's mutable state.
    async fn update_run(&self, run: &Run) -> Result<()>;
    /// List a run's direct children.
    async fn list_children(&self, parent: RunId) -> Result<Vec<Run>>;
    /// Atomically take an exclusive processing lease on the run so a second queue
    /// entry for it can't drive a concurrent `advance`. Returns `false` if a live
    /// lease is already held; a crashed holder's lease expires after `lease_ttl`,
    /// letting another worker re-acquire. Independent of the queue-entry lease,
    /// which guards a row, not the run.
    async fn try_acquire_run(&self, id: RunId, lease_ttl: Duration) -> Result<bool>;
    /// Release the processing lease taken by [`Self::try_acquire_run`].
    async fn release_run(&self, id: RunId) -> Result<()>;
}

/// The append-only event log.
#[async_trait]
pub trait EventLog: Send + Sync {
    /// Append an event, assigning it the next sequence number. Returns that seq.
    async fn append(
        &self,
        run_id: RunId,
        kind: EventKind,
        payload: serde_json::Value,
    ) -> Result<u64>;
    /// Read events for a run with `seq > from_seq`, in order.
    async fn read(&self, run_id: RunId, from_seq: u64) -> Result<Vec<Event>>;
}

/// The durable-step idempotency journal.
#[async_trait]
pub trait StepStore: Send + Sync {
    /// Look up a journaled step result.
    async fn lookup_step(&self, run_id: RunId, key: &str) -> Result<Option<StepRecord>>;
    /// Insert or overwrite a journaled step result.
    async fn record_step(&self, record: &StepRecord) -> Result<()>;
}

/// The work queue with leased, redeliverable claims.
#[async_trait]
pub trait Queue: Send + Sync {
    /// Make a run runnable, optionally not before `ready_at`.
    async fn enqueue(&self, run_id: RunId, ready_at: Option<DateTime<Utc>>) -> Result<()>;
    /// Claim the next ready run for `lease_ttl`, or `None` if nothing is ready.
    async fn claim(&self, worker_id: &str, lease_ttl: Duration) -> Result<Option<Lease<RunId>>>;
    /// Acknowledge a processed claim, removing it from the queue.
    async fn ack(&self, lease: &Lease<RunId>) -> Result<()>;
}

/// Durable signal delivery.
#[async_trait]
pub trait SignalStore: Send + Sync {
    /// Deliver a signal to a run.
    async fn deliver(&self, run_id: RunId, name: &str, payload: serde_json::Value) -> Result<()>;
    /// Atomically consume and return the earliest unconsumed signal named `name`,
    /// or `None` if there is none. Other unconsumed signals are left intact, so a
    /// run never loses a delivered signal it did not take this turn.
    async fn take_signal(&self, run_id: RunId, name: &str) -> Result<Option<Signal>>;
    /// Whether the run has an unconsumed signal with the given name (no consume).
    async fn has_unconsumed(&self, run_id: RunId, name: &str) -> Result<bool>;
}

/// Leased-resource records.
#[async_trait]
pub trait ResourceStore: Send + Sync {
    /// Record a newly leased resource.
    async fn lease_resource(&self, resource: &Resource) -> Result<()>;
    /// Extend a resource lease.
    async fn heartbeat(&self, id: ResourceId, expires_at: DateTime<Utc>) -> Result<()>;
    /// Mark a resource released.
    async fn release_resource(&self, id: ResourceId) -> Result<()>;
    /// List a run's resources.
    async fn list_resources(&self, run_id: RunId) -> Result<Vec<Resource>>;
}

/// Artifact records.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Record a produced artifact.
    async fn put_artifact(&self, artifact: &Artifact) -> Result<()>;
    /// List a run's artifacts.
    async fn list_artifacts(&self, run_id: RunId) -> Result<Vec<Artifact>>;
}

/// The full storage surface the engine needs. Blanket-implemented for any type
/// that implements every constituent trait, so backends just implement the parts.
pub trait Backend:
    RunStore + EventLog + StepStore + Queue + SignalStore + ResourceStore + ArtifactStore + Clock
{
}

impl<T> Backend for T where
    T: RunStore
        + EventLog
        + StepStore
        + Queue
        + SignalStore
        + ResourceStore
        + ArtifactStore
        + Clock
{
}
