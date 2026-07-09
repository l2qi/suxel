// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! A SQLite [`Backend`](suxel_core::store::Backend) for the Suxel runtime.
//!
//! Uses `rusqlite` (bundled) + an `r2d2` connection pool. Suitable for local dev
//! and embedded single-node use;
//! for multi-tenant production use the Postgres backend.
//!
//! Complex fields (budget, usage, waiting, payloads, metadata) are stored as JSON
//! text; timestamps as epoch-millis integers. The work queue uses a `queue` table
//! with a `leased_until` column and `BEGIN IMMEDIATE` claims for atomicity.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use std::path::Path;
use std::time::Duration;
use suxel_core::artifact::Artifact;
use suxel_core::error::{Error, Result};
use suxel_core::event::{Event, EventKind};
use suxel_core::ids::{ArtifactId, ResourceId, RunId};
use suxel_core::resource::{Resource, ResourceStatus};
use suxel_core::run::Run;
use suxel_core::signal::Signal;
use suxel_core::sql_encoding::{
    dt, ms, opt_value_to_json, parse_run_id, run_from_row, to_json, RunRow, RUN_COLUMNS,
};
use suxel_core::step::{StepKey, StepRecord};
use suxel_core::store::{
    ArtifactStore, Clock, EventLog, Lease, Queue, ResourceStore, RunStore, SignalStore, StepStore,
};

type Conn = r2d2::PooledConnection<SqliteConnectionManager>;

/// A SQLite-backed durable runtime store.
pub struct SqliteStore {
    pool: Pool<SqliteConnectionManager>,
}

impl SqliteStore {
    /// Open (creating if needed) a store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let manager = SqliteConnectionManager::file(path).with_init(|c| {
            c.execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=OFF;",
            )
        });
        Self::from_manager(manager)
    }

    /// Open an in-memory store (single shared connection; for tests).
    pub fn in_memory() -> Result<Self> {
        let manager = SqliteConnectionManager::memory();
        let pool = Pool::builder()
            .max_size(1)
            .build(manager)
            .map_err(Error::storage)?;
        let store = SqliteStore { pool };
        store.init_schema()?;
        Ok(store)
    }

    fn from_manager(manager: SqliteConnectionManager) -> Result<Self> {
        let pool = Pool::builder().build(manager).map_err(Error::storage)?;
        let store = SqliteStore { pool };
        store.init_schema()?;
        Ok(store)
    }

    fn conn(&self) -> Result<Conn> {
        self.pool.get().map_err(Error::storage)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn()?.execute_batch(SCHEMA).map_err(Error::storage)?;
        Ok(())
    }
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS runs (
    id          TEXT PRIMARY KEY,
    parent_id   TEXT,
    agent_type  TEXT NOT NULL,
    goal        TEXT NOT NULL,
    input       TEXT,
    status      TEXT NOT NULL,
    waiting     TEXT,
    session_ref TEXT,
    budget      TEXT NOT NULL,
    usage       TEXT NOT NULL,
    output      TEXT,
    error       TEXT,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_parent ON runs(parent_id);

CREATE TABLE IF NOT EXISTS events (
    run_id   TEXT NOT NULL,
    seq      INTEGER NOT NULL,
    kind     TEXT NOT NULL,
    payload  TEXT NOT NULL,
    at       INTEGER NOT NULL,
    PRIMARY KEY (run_id, seq)
);

CREATE TABLE IF NOT EXISTS durable_steps (
    run_id     TEXT NOT NULL,
    key        TEXT NOT NULL,
    status     TEXT NOT NULL,
    output     TEXT,
    error      TEXT,
    attempts   INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (run_id, key)
);

CREATE TABLE IF NOT EXISTS signals (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id     TEXT NOT NULL,
    name       TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    consumed   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_signals_run ON signals(run_id, consumed);

CREATE TABLE IF NOT EXISTS resources (
    id               TEXT PRIMARY KEY,
    run_id           TEXT NOT NULL,
    kind             TEXT NOT NULL,
    status           TEXT NOT NULL,
    metadata         TEXT NOT NULL,
    lease_expires_at INTEGER,
    created_at       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_resources_run ON resources(run_id);

CREATE TABLE IF NOT EXISTS artifacts (
    id         TEXT PRIMARY KEY,
    run_id     TEXT NOT NULL,
    kind       TEXT NOT NULL,
    uri        TEXT NOT NULL,
    metadata   TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artifacts_run ON artifacts(run_id);

CREATE TABLE IF NOT EXISTS queue (
    entry_id     TEXT PRIMARY KEY,
    run_id       TEXT NOT NULL,
    ready_at     INTEGER NOT NULL,
    leased_until INTEGER
);
CREATE INDEX IF NOT EXISTS idx_queue_ready ON queue(ready_at);
"#;

// ---- run row reader ---------------------------------------------------------

fn read_raw_run(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRow> {
    Ok(RunRow {
        id: row.get(0)?,
        parent_id: row.get(1)?,
        agent_type: row.get(2)?,
        goal: row.get(3)?,
        input: row.get(4)?,
        status: row.get(5)?,
        waiting: row.get(6)?,
        session_ref: row.get(7)?,
        budget: row.get(8)?,
        usage: row.get(9)?,
        output: row.get(10)?,
        error: row.get(11)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
    })
}

// ---- RunStore ---------------------------------------------------------------

#[async_trait]
impl RunStore for SqliteStore {
    async fn create_run(&self, run: &Run) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "INSERT INTO runs ({RUN_COLUMNS}) VALUES \
                      (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)"
            ),
            params![
                run.id.to_string(),
                run.parent_id.map(|p| p.to_string()),
                run.agent_type,
                run.goal,
                opt_value_to_json(&run.input),
                to_json(&run.status)?,
                run.waiting.as_ref().map(to_json).transpose()?,
                run.session_ref,
                to_json(&run.budget)?,
                to_json(&run.usage)?,
                opt_value_to_json(&run.output),
                run.error,
                ms(run.created_at),
                ms(run.updated_at),
            ],
        )
        .map_err(Error::storage)?;
        Ok(())
    }

    async fn get_run(&self, id: RunId) -> Result<Run> {
        let conn = self.conn()?;
        let raw = conn
            .query_row(
                &format!("SELECT {RUN_COLUMNS} FROM runs WHERE id=?1"),
                params![id.to_string()],
                read_raw_run,
            )
            .optional()
            .map_err(Error::storage)?;
        raw.ok_or(Error::RunNotFound(id)).and_then(run_from_row)
    }

    async fn update_run(&self, run: &Run) -> Result<()> {
        let conn = self.conn()?;
        let affected = conn
            .execute(
                "UPDATE runs SET parent_id=?2, agent_type=?3, goal=?4, input=?5, status=?6, \
                 waiting=?7, session_ref=?8, budget=?9, usage=?10, output=?11, error=?12, \
                 created_at=?13, updated_at=?14 WHERE id=?1",
                params![
                    run.id.to_string(),
                    run.parent_id.map(|p| p.to_string()),
                    run.agent_type,
                    run.goal,
                    opt_value_to_json(&run.input),
                    to_json(&run.status)?,
                    run.waiting.as_ref().map(to_json).transpose()?,
                    run.session_ref,
                    to_json(&run.budget)?,
                    to_json(&run.usage)?,
                    opt_value_to_json(&run.output),
                    run.error,
                    ms(run.created_at),
                    ms(run.updated_at),
                ],
            )
            .map_err(Error::storage)?;
        if affected == 0 {
            return Err(Error::RunNotFound(run.id));
        }
        Ok(())
    }

    async fn list_children(&self, parent: RunId) -> Result<Vec<Run>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(&format!(
                "SELECT {RUN_COLUMNS} FROM runs WHERE parent_id=?1 ORDER BY created_at"
            ))
            .map_err(Error::storage)?;
        let rows = stmt
            .query_map(params![parent.to_string()], read_raw_run)
            .map_err(Error::storage)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(run_from_row(r.map_err(Error::storage)?)?);
        }
        Ok(out)
    }
}

// ---- EventLog ---------------------------------------------------------------

#[async_trait]
impl EventLog for SqliteStore {
    async fn append(
        &self,
        run_id: RunId,
        kind: EventKind,
        payload: serde_json::Value,
    ) -> Result<u64> {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Error::storage)?;
        let seq: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(seq),0)+1 FROM events WHERE run_id=?1",
                params![run_id.to_string()],
                |r| r.get(0),
            )
            .map_err(Error::storage)?;
        tx.execute(
            "INSERT INTO events (run_id, seq, kind, payload, at) VALUES (?1,?2,?3,?4,?5)",
            params![
                run_id.to_string(),
                seq,
                to_json(&kind)?,
                payload.to_string(),
                ms(Utc::now()),
            ],
        )
        .map_err(Error::storage)?;
        tx.commit().map_err(Error::storage)?;
        Ok(seq as u64)
    }

    async fn read(&self, run_id: RunId, from_seq: u64) -> Result<Vec<Event>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT seq, kind, payload, at FROM events WHERE run_id=?1 AND seq>?2 ORDER BY seq",
            )
            .map_err(Error::storage)?;
        let rows = stmt
            .query_map(params![run_id.to_string(), from_seq as i64], |row| {
                let seq: i64 = row.get(0)?;
                let kind: String = row.get(1)?;
                let payload: String = row.get(2)?;
                let at: i64 = row.get(3)?;
                Ok((seq, kind, payload, at))
            })
            .map_err(Error::storage)?;
        let mut out = Vec::new();
        for r in rows {
            let (seq, kind, payload, at) = r.map_err(Error::storage)?;
            out.push(Event {
                run_id,
                seq: seq as u64,
                kind: serde_json::from_str(&kind)?,
                payload: serde_json::from_str(&payload)?,
                at: dt(at),
            });
        }
        Ok(out)
    }
}

// ---- StepStore --------------------------------------------------------------

#[async_trait]
impl StepStore for SqliteStore {
    async fn lookup_step(&self, run_id: RunId, key: &str) -> Result<Option<StepRecord>> {
        let conn = self.conn()?;
        let row = conn
            .query_row(
                "SELECT status, output, error, attempts, updated_at FROM durable_steps \
                 WHERE run_id=?1 AND key=?2",
                params![run_id.to_string(), key],
                |row| {
                    let status: String = row.get(0)?;
                    let output: Option<String> = row.get(1)?;
                    let error: Option<String> = row.get(2)?;
                    let attempts: i64 = row.get(3)?;
                    let updated_at: i64 = row.get(4)?;
                    Ok((status, output, error, attempts, updated_at))
                },
            )
            .optional()
            .map_err(Error::storage)?;
        let Some((status, output, error, attempts, updated_at)) = row else {
            return Ok(None);
        };
        Ok(Some(StepRecord {
            run_id,
            key: StepKey(key.to_string()),
            status: serde_json::from_str(&status)?,
            output: output.map(|s| serde_json::from_str(&s)).transpose()?,
            error,
            attempts: attempts as u32,
            updated_at: dt(updated_at),
        }))
    }

    async fn record_step(&self, record: &StepRecord) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO durable_steps (run_id, key, status, output, error, attempts, updated_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7) \
             ON CONFLICT(run_id, key) DO UPDATE SET \
               status=excluded.status, output=excluded.output, error=excluded.error, \
               attempts=excluded.attempts, updated_at=excluded.updated_at",
            params![
                record.run_id.to_string(),
                record.key.0,
                to_json(&record.status)?,
                opt_value_to_json(&record.output),
                record.error,
                record.attempts as i64,
                ms(record.updated_at),
            ],
        )
        .map_err(Error::storage)?;
        Ok(())
    }
}

// ---- Queue ------------------------------------------------------------------

#[async_trait]
impl Queue for SqliteStore {
    async fn enqueue(&self, run_id: RunId, ready_at: Option<DateTime<Utc>>) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO queue (entry_id, run_id, ready_at, leased_until) VALUES (?1,?2,?3,NULL)",
            params![
                uuid::Uuid::now_v7().to_string(),
                run_id.to_string(),
                ms(ready_at.unwrap_or_else(Utc::now)),
            ],
        )
        .map_err(Error::storage)?;
        Ok(())
    }

    async fn claim(&self, _worker_id: &str, lease_ttl: Duration) -> Result<Option<Lease<RunId>>> {
        let mut conn = self.conn()?;
        let now = ms(Utc::now());
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Error::storage)?;
        let claimed = tx
            .query_row(
                "SELECT entry_id, run_id FROM queue \
                 WHERE ready_at<=?1 AND (leased_until IS NULL OR leased_until<=?1) \
                 ORDER BY ready_at LIMIT 1",
                params![now],
                |row| {
                    let entry_id: String = row.get(0)?;
                    let run_id: String = row.get(1)?;
                    Ok((entry_id, run_id))
                },
            )
            .optional()
            .map_err(Error::storage)?;
        let Some((entry_id, run_id)) = claimed else {
            tx.commit().map_err(Error::storage)?;
            return Ok(None);
        };
        let expires = now + lease_ttl.as_millis() as i64;
        tx.execute(
            "UPDATE queue SET leased_until=?2 WHERE entry_id=?1",
            params![entry_id, expires],
        )
        .map_err(Error::storage)?;
        tx.commit().map_err(Error::storage)?;
        Ok(Some(Lease {
            item: parse_run_id(&run_id)?,
            lease_id: entry_id.parse().map_err(Error::storage)?,
            expires_at: dt(expires),
        }))
    }

    async fn ack(&self, lease: &Lease<RunId>) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "DELETE FROM queue WHERE entry_id=?1",
            params![lease.lease_id.to_string()],
        )
        .map_err(Error::storage)?;
        Ok(())
    }
}

// ---- SignalStore ------------------------------------------------------------

#[async_trait]
impl SignalStore for SqliteStore {
    async fn deliver(&self, run_id: RunId, name: &str, payload: serde_json::Value) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO signals (run_id, name, payload, created_at, consumed) \
             VALUES (?1,?2,?3,?4,0)",
            params![
                run_id.to_string(),
                name,
                payload.to_string(),
                ms(Utc::now())
            ],
        )
        .map_err(Error::storage)?;
        Ok(())
    }

    async fn take_signal(&self, run_id: RunId, name: &str) -> Result<Option<Signal>> {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Error::storage)?;
        let row = tx
            .query_row(
                "SELECT id, payload, created_at FROM signals \
                 WHERE run_id=?1 AND name=?2 AND consumed=0 ORDER BY id LIMIT 1",
                params![run_id.to_string(), name],
                |row| {
                    let id: i64 = row.get(0)?;
                    let payload: String = row.get(1)?;
                    let created_at: i64 = row.get(2)?;
                    Ok((id, payload, created_at))
                },
            )
            .optional()
            .map_err(Error::storage)?;
        let result = match row {
            Some((id, payload, created_at)) => {
                tx.execute("UPDATE signals SET consumed=1 WHERE id=?1", params![id])
                    .map_err(Error::storage)?;
                Some(Signal {
                    run_id,
                    name: name.to_string(),
                    payload: serde_json::from_str(&payload)?,
                    created_at: dt(created_at),
                    consumed: true,
                })
            }
            None => None,
        };
        tx.commit().map_err(Error::storage)?;
        Ok(result)
    }

    async fn has_unconsumed(&self, run_id: RunId, name: &str) -> Result<bool> {
        let conn = self.conn()?;
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM signals WHERE run_id=?1 AND name=?2 AND consumed=0)",
                params![run_id.to_string(), name],
                |r| r.get(0),
            )
            .map_err(Error::storage)?;
        Ok(exists)
    }
}

// ---- ResourceStore ----------------------------------------------------------

#[async_trait]
impl ResourceStore for SqliteStore {
    async fn lease_resource(&self, resource: &Resource) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO resources (id, run_id, kind, status, metadata, lease_expires_at, created_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                resource.id.to_string(),
                resource.run_id.to_string(),
                to_json(&resource.kind)?,
                to_json(&resource.status)?,
                resource.metadata.to_string(),
                resource.lease_expires_at.map(ms),
                ms(resource.created_at),
            ],
        )
        .map_err(Error::storage)?;
        Ok(())
    }

    async fn heartbeat(&self, id: ResourceId, expires_at: DateTime<Utc>) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE resources SET lease_expires_at=?2 WHERE id=?1",
            params![id.to_string(), ms(expires_at)],
        )
        .map_err(Error::storage)?;
        Ok(())
    }

    async fn release_resource(&self, id: ResourceId) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE resources SET status=?2 WHERE id=?1",
            params![id.to_string(), to_json(&ResourceStatus::Released)?],
        )
        .map_err(Error::storage)?;
        Ok(())
    }

    async fn list_resources(&self, run_id: RunId) -> Result<Vec<Resource>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, kind, status, metadata, lease_expires_at, created_at \
                 FROM resources WHERE run_id=?1 ORDER BY created_at",
            )
            .map_err(Error::storage)?;
        let rows = stmt
            .query_map(params![run_id.to_string()], |row| {
                let id: String = row.get(0)?;
                let kind: String = row.get(1)?;
                let status: String = row.get(2)?;
                let metadata: String = row.get(3)?;
                let lease: Option<i64> = row.get(4)?;
                let created_at: i64 = row.get(5)?;
                Ok((id, kind, status, metadata, lease, created_at))
            })
            .map_err(Error::storage)?;
        let mut out = Vec::new();
        for r in rows {
            let (id, kind, status, metadata, lease, created_at) = r.map_err(Error::storage)?;
            out.push(Resource {
                id: id.parse::<ResourceId>().map_err(Error::storage)?,
                run_id,
                kind: serde_json::from_str(&kind)?,
                status: serde_json::from_str(&status)?,
                metadata: serde_json::from_str(&metadata)?,
                lease_expires_at: lease.map(dt),
                created_at: dt(created_at),
            });
        }
        Ok(out)
    }
}

// ---- ArtifactStore ----------------------------------------------------------

#[async_trait]
impl ArtifactStore for SqliteStore {
    async fn put_artifact(&self, artifact: &Artifact) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO artifacts (id, run_id, kind, uri, metadata, created_at) \
             VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                artifact.id.to_string(),
                artifact.run_id.to_string(),
                artifact.kind,
                artifact.uri,
                artifact.metadata.to_string(),
                ms(artifact.created_at),
            ],
        )
        .map_err(Error::storage)?;
        Ok(())
    }

    async fn list_artifacts(&self, run_id: RunId) -> Result<Vec<Artifact>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, kind, uri, metadata, created_at FROM artifacts \
                 WHERE run_id=?1 ORDER BY created_at",
            )
            .map_err(Error::storage)?;
        let rows = stmt
            .query_map(params![run_id.to_string()], |row| {
                let id: String = row.get(0)?;
                let kind: String = row.get(1)?;
                let uri: String = row.get(2)?;
                let metadata: String = row.get(3)?;
                let created_at: i64 = row.get(4)?;
                Ok((id, kind, uri, metadata, created_at))
            })
            .map_err(Error::storage)?;
        let mut out = Vec::new();
        for r in rows {
            let (id, kind, uri, metadata, created_at) = r.map_err(Error::storage)?;
            out.push(Artifact {
                id: id.parse::<ArtifactId>().map_err(Error::storage)?,
                run_id,
                kind,
                uri,
                metadata: serde_json::from_str(&metadata)?,
                created_at: dt(created_at),
            });
        }
        Ok(out)
    }
}

// ---- Clock ------------------------------------------------------------------

impl Clock for SqliteStore {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
