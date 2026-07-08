// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! An in-memory [`Backend`](crate::store::Backend) for tests, examples, and
//! embedded single-process use.
//!
//! It is a faithful (if simple) implementation of the full storage surface,
//! including a delayed-ready, lease-based queue with redelivery — enough to
//! exercise crash-recovery and timer semantics in tests.

use crate::artifact::Artifact;
use crate::error::{Error, Result};
use crate::event::{Event, EventKind};
use crate::ids::{ResourceId, RunId};
use crate::resource::{Resource, ResourceStatus};
use crate::run::Run;
use crate::signal::Signal;
use crate::step::StepRecord;
use crate::store::{
    ArtifactStore, Clock, EventLog, Lease, Queue, ResourceStore, RunStore, SignalStore, StepStore,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// A queue entry. The `entry_id` doubles as the lease handle: a claim leases the
/// entry in place; `ack` removes it by id. Enqueues made while processing create
/// fresh entries, so acking the claimed entry never drops them.
struct QueueEntry {
    entry_id: uuid::Uuid,
    run_id: RunId,
    ready_at: DateTime<Utc>,
    leased_until: Option<DateTime<Utc>>,
}

#[derive(Default)]
struct Inner {
    runs: HashMap<RunId, Run>,
    events: HashMap<RunId, Vec<Event>>,
    steps: HashMap<(RunId, String), StepRecord>,
    signals: HashMap<RunId, Vec<Signal>>,
    resources: HashMap<ResourceId, Resource>,
    artifacts: HashMap<RunId, Vec<Artifact>>,
    queue: Vec<QueueEntry>,
}

/// In-memory backend. Cheap to clone-wrap in an `Arc`.
#[derive(Default)]
pub struct InMemoryBackend {
    inner: Mutex<Inner>,
}

impl InMemoryBackend {
    /// Create an empty backend.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("suxel in-memory backend poisoned")
    }
}

impl Clock for InMemoryBackend {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[async_trait]
impl RunStore for InMemoryBackend {
    async fn create_run(&self, run: &Run) -> Result<()> {
        self.lock().runs.insert(run.id, run.clone());
        Ok(())
    }

    async fn get_run(&self, id: RunId) -> Result<Run> {
        self.lock()
            .runs
            .get(&id)
            .cloned()
            .ok_or(Error::RunNotFound(id))
    }

    async fn update_run(&self, run: &Run) -> Result<()> {
        let mut g = self.lock();
        if !g.runs.contains_key(&run.id) {
            return Err(Error::RunNotFound(run.id));
        }
        g.runs.insert(run.id, run.clone());
        Ok(())
    }

    async fn list_children(&self, parent: RunId) -> Result<Vec<Run>> {
        let g = self.lock();
        let mut kids: Vec<Run> = g
            .runs
            .values()
            .filter(|r| r.parent_id == Some(parent))
            .cloned()
            .collect();
        kids.sort_by_key(|r| r.created_at);
        Ok(kids)
    }
}

#[async_trait]
impl EventLog for InMemoryBackend {
    async fn append(
        &self,
        run_id: RunId,
        kind: EventKind,
        payload: serde_json::Value,
    ) -> Result<u64> {
        let mut g = self.lock();
        let log = g.events.entry(run_id).or_default();
        let seq = log.len() as u64 + 1;
        log.push(Event {
            run_id,
            seq,
            kind,
            payload,
            at: Utc::now(),
        });
        Ok(seq)
    }

    async fn read(&self, run_id: RunId, from_seq: u64) -> Result<Vec<Event>> {
        let g = self.lock();
        Ok(g.events
            .get(&run_id)
            .map(|log| log.iter().filter(|e| e.seq > from_seq).cloned().collect())
            .unwrap_or_default())
    }
}

#[async_trait]
impl StepStore for InMemoryBackend {
    async fn lookup_step(&self, run_id: RunId, key: &str) -> Result<Option<StepRecord>> {
        Ok(self.lock().steps.get(&(run_id, key.to_string())).cloned())
    }

    async fn record_step(&self, record: &StepRecord) -> Result<()> {
        self.lock()
            .steps
            .insert((record.run_id, record.key.0.clone()), record.clone());
        Ok(())
    }
}

#[async_trait]
impl Queue for InMemoryBackend {
    async fn enqueue(&self, run_id: RunId, ready_at: Option<DateTime<Utc>>) -> Result<()> {
        let mut g = self.lock();
        g.queue.push(QueueEntry {
            entry_id: uuid::Uuid::now_v7(),
            run_id,
            ready_at: ready_at.unwrap_or_else(Utc::now),
            leased_until: None,
        });
        Ok(())
    }

    async fn claim(&self, _worker_id: &str, lease_ttl: Duration) -> Result<Option<Lease<RunId>>> {
        let mut g = self.lock();
        let now = Utc::now();
        // Pick the earliest-ready claimable entry: ready and either unleased or
        // with an expired lease (redelivery after a crash).
        let mut best: Option<usize> = None;
        for (i, e) in g.queue.iter().enumerate() {
            let claimable =
                e.ready_at <= now && e.leased_until.map(|exp| exp <= now).unwrap_or(true);
            if !claimable {
                continue;
            }
            match best {
                Some(b) if g.queue[b].ready_at <= e.ready_at => {}
                _ => best = Some(i),
            }
        }
        let Some(idx) = best else {
            return Ok(None);
        };
        let ttl =
            chrono::Duration::from_std(lease_ttl).unwrap_or_else(|_| chrono::Duration::seconds(30));
        let expires_at = now + ttl;
        let entry = &mut g.queue[idx];
        entry.leased_until = Some(expires_at);
        Ok(Some(Lease {
            item: entry.run_id,
            lease_id: entry.entry_id,
            expires_at,
        }))
    }

    async fn ack(&self, lease: &Lease<RunId>) -> Result<()> {
        self.lock().queue.retain(|e| e.entry_id != lease.lease_id);
        Ok(())
    }
}

#[async_trait]
impl SignalStore for InMemoryBackend {
    async fn deliver(&self, run_id: RunId, name: &str, payload: serde_json::Value) -> Result<()> {
        self.lock().signals.entry(run_id).or_default().push(Signal {
            run_id,
            name: name.to_string(),
            payload,
            created_at: Utc::now(),
            consumed: false,
        });
        Ok(())
    }

    async fn take_signal(&self, run_id: RunId, name: &str) -> Result<Option<Signal>> {
        let mut g = self.lock();
        let Some(sigs) = g.signals.get_mut(&run_id) else {
            return Ok(None);
        };
        for s in sigs.iter_mut() {
            if !s.consumed && s.name == name {
                s.consumed = true;
                return Ok(Some(s.clone()));
            }
        }
        Ok(None)
    }

    async fn has_unconsumed(&self, run_id: RunId, name: &str) -> Result<bool> {
        let g = self.lock();
        Ok(g.signals
            .get(&run_id)
            .map(|sigs| sigs.iter().any(|s| !s.consumed && s.name == name))
            .unwrap_or(false))
    }
}

#[async_trait]
impl ResourceStore for InMemoryBackend {
    async fn lease_resource(&self, resource: &Resource) -> Result<()> {
        self.lock().resources.insert(resource.id, resource.clone());
        Ok(())
    }

    async fn heartbeat(&self, id: ResourceId, expires_at: DateTime<Utc>) -> Result<()> {
        let mut g = self.lock();
        if let Some(r) = g.resources.get_mut(&id) {
            r.lease_expires_at = Some(expires_at);
        }
        Ok(())
    }

    async fn release_resource(&self, id: ResourceId) -> Result<()> {
        let mut g = self.lock();
        if let Some(r) = g.resources.get_mut(&id) {
            r.status = ResourceStatus::Released;
        }
        Ok(())
    }

    async fn list_resources(&self, run_id: RunId) -> Result<Vec<Resource>> {
        let g = self.lock();
        Ok(g.resources
            .values()
            .filter(|r| r.run_id == run_id)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl ArtifactStore for InMemoryBackend {
    async fn put_artifact(&self, artifact: &Artifact) -> Result<()> {
        self.lock()
            .artifacts
            .entry(artifact.run_id)
            .or_default()
            .push(artifact.clone());
        Ok(())
    }

    async fn list_artifacts(&self, run_id: RunId) -> Result<Vec<Artifact>> {
        Ok(self
            .lock()
            .artifacts
            .get(&run_id)
            .cloned()
            .unwrap_or_default())
    }
}
