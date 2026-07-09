// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! A Postgres [`Backend`](suxel_core::store::Backend) for the Suxel runtime.
//!
//! Uses `sqlx` with the runtime query API (no compile-time DB needed to build).
//! The work queue claims with `SELECT … FOR UPDATE SKIP LOCKED`, giving real
//! multi-worker concurrency — the production path for a multi-tenant deployment.
//!
//! Mirrors the SQLite backend's logical model: complex fields as JSON text,
//! timestamps as `BIGINT` epoch-millis, ids as `TEXT`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool};
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

/// A Postgres-backed durable runtime store. Cheap to clone (shares the pool).
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Connect to `url` (with a pool) and ensure the schema exists.
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(url)
            .await
            .map_err(Error::storage)?;
        Self::from_pool(pool).await
    }

    /// Build a store over an existing pool, ensuring the schema exists.
    pub async fn from_pool(pool: PgPool) -> Result<Self> {
        sqlx::raw_sql(SCHEMA)
            .execute(&pool)
            .await
            .map_err(Error::storage)?;
        Ok(PostgresStore { pool })
    }

    /// Truncate every runtime table. Intended for tests.
    pub async fn reset(&self) -> Result<()> {
        sqlx::raw_sql(
            "TRUNCATE runs, events, durable_steps, signals, resources, artifacts, queue;",
        )
        .execute(&self.pool)
        .await
        .map_err(Error::storage)?;
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
    created_at  BIGINT NOT NULL,
    updated_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_parent ON runs(parent_id);

CREATE TABLE IF NOT EXISTS events (
    run_id   TEXT NOT NULL,
    seq      BIGINT NOT NULL,
    kind     TEXT NOT NULL,
    payload  TEXT NOT NULL,
    at       BIGINT NOT NULL,
    PRIMARY KEY (run_id, seq)
);

CREATE TABLE IF NOT EXISTS durable_steps (
    run_id     TEXT NOT NULL,
    key        TEXT NOT NULL,
    status     TEXT NOT NULL,
    output     TEXT,
    error      TEXT,
    attempts   BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (run_id, key)
);

CREATE TABLE IF NOT EXISTS signals (
    id         BIGSERIAL PRIMARY KEY,
    run_id     TEXT NOT NULL,
    name       TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    consumed   BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_signals_run ON signals(run_id, consumed);

CREATE TABLE IF NOT EXISTS resources (
    id               TEXT PRIMARY KEY,
    run_id           TEXT NOT NULL,
    kind             TEXT NOT NULL,
    status           TEXT NOT NULL,
    metadata         TEXT NOT NULL,
    lease_expires_at BIGINT,
    created_at       BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_resources_run ON resources(run_id);

CREATE TABLE IF NOT EXISTS artifacts (
    id         TEXT PRIMARY KEY,
    run_id     TEXT NOT NULL,
    kind       TEXT NOT NULL,
    uri        TEXT NOT NULL,
    metadata   TEXT NOT NULL,
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artifacts_run ON artifacts(run_id);

CREATE TABLE IF NOT EXISTS queue (
    entry_id     TEXT PRIMARY KEY,
    run_id       TEXT NOT NULL,
    ready_at     BIGINT NOT NULL,
    leased_until BIGINT
);
CREATE INDEX IF NOT EXISTS idx_queue_ready ON queue(ready_at);
"#;

// ---- run row reader ---------------------------------------------------------

/// `sqlx::FromRow` carrier for a `runs` row; converted to the shared [`RunRow`]
/// (`FromRow` can't be derived on the core type from this crate).
#[derive(FromRow)]
struct RawRun {
    id: String,
    parent_id: Option<String>,
    agent_type: String,
    goal: String,
    input: Option<String>,
    status: String,
    waiting: Option<String>,
    session_ref: Option<String>,
    budget: String,
    usage: String,
    output: Option<String>,
    error: Option<String>,
    created_at: i64,
    updated_at: i64,
}

impl From<RawRun> for RunRow {
    fn from(r: RawRun) -> Self {
        RunRow {
            id: r.id,
            parent_id: r.parent_id,
            agent_type: r.agent_type,
            goal: r.goal,
            input: r.input,
            status: r.status,
            waiting: r.waiting,
            session_ref: r.session_ref,
            budget: r.budget,
            usage: r.usage,
            output: r.output,
            error: r.error,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

// ---- RunStore ---------------------------------------------------------------

#[async_trait]
impl RunStore for PostgresStore {
    async fn create_run(&self, run: &Run) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO runs ({RUN_COLUMNS}) VALUES \
             ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)"
        ))
        .bind(run.id.to_string())
        .bind(run.parent_id.map(|p| p.to_string()))
        .bind(&run.agent_type)
        .bind(&run.goal)
        .bind(opt_value_to_json(&run.input))
        .bind(to_json(&run.status)?)
        .bind(run.waiting.as_ref().map(to_json).transpose()?)
        .bind(&run.session_ref)
        .bind(to_json(&run.budget)?)
        .bind(to_json(&run.usage)?)
        .bind(opt_value_to_json(&run.output))
        .bind(&run.error)
        .bind(ms(run.created_at))
        .bind(ms(run.updated_at))
        .execute(&self.pool)
        .await
        .map_err(Error::storage)?;
        Ok(())
    }

    async fn get_run(&self, id: RunId) -> Result<Run> {
        let raw =
            sqlx::query_as::<_, RawRun>(&format!("SELECT {RUN_COLUMNS} FROM runs WHERE id=$1"))
                .bind(id.to_string())
                .fetch_optional(&self.pool)
                .await
                .map_err(Error::storage)?;
        raw.ok_or(Error::RunNotFound(id))
            .and_then(|r| run_from_row(r.into()))
    }

    async fn update_run(&self, run: &Run) -> Result<()> {
        let result = sqlx::query(
            "UPDATE runs SET parent_id=$2, agent_type=$3, goal=$4, input=$5, status=$6, \
             waiting=$7, session_ref=$8, budget=$9, usage=$10, output=$11, error=$12, \
             created_at=$13, updated_at=$14 WHERE id=$1",
        )
        .bind(run.id.to_string())
        .bind(run.parent_id.map(|p| p.to_string()))
        .bind(&run.agent_type)
        .bind(&run.goal)
        .bind(opt_value_to_json(&run.input))
        .bind(to_json(&run.status)?)
        .bind(run.waiting.as_ref().map(to_json).transpose()?)
        .bind(&run.session_ref)
        .bind(to_json(&run.budget)?)
        .bind(to_json(&run.usage)?)
        .bind(opt_value_to_json(&run.output))
        .bind(&run.error)
        .bind(ms(run.created_at))
        .bind(ms(run.updated_at))
        .execute(&self.pool)
        .await
        .map_err(Error::storage)?;
        if result.rows_affected() == 0 {
            return Err(Error::RunNotFound(run.id));
        }
        Ok(())
    }

    async fn list_children(&self, parent: RunId) -> Result<Vec<Run>> {
        let raws = sqlx::query_as::<_, RawRun>(&format!(
            "SELECT {RUN_COLUMNS} FROM runs WHERE parent_id=$1 ORDER BY created_at"
        ))
        .bind(parent.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(Error::storage)?;
        raws.into_iter().map(|r| run_from_row(r.into())).collect()
    }
}

// ---- EventLog ---------------------------------------------------------------

#[async_trait]
impl EventLog for PostgresStore {
    async fn append(
        &self,
        run_id: RunId,
        kind: EventKind,
        payload: serde_json::Value,
    ) -> Result<u64> {
        // Assign the next seq inside a transaction that first takes a per-run
        // advisory lock. External callers (signal/approve/cancel) append events
        // concurrently with a worker tick on the same run, and `MAX(seq)+1` read
        // under MVCC would otherwise let two appends compute the same seq and
        // collide on the (run_id, seq) primary key. The lock serializes appends
        // per run (it releases at COMMIT); appends to other runs are unaffected.
        let mut tx = self.pool.begin().await.map_err(Error::storage)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
            .bind(run_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(Error::storage)?;
        let seq: i64 = sqlx::query_scalar(
            "INSERT INTO events (run_id, seq, kind, payload, at) \
             VALUES ($1, (SELECT COALESCE(MAX(seq),0)+1 FROM events WHERE run_id=$1), $2, $3, $4) \
             RETURNING seq",
        )
        .bind(run_id.to_string())
        .bind(to_json(&kind)?)
        .bind(payload.to_string())
        .bind(ms(Utc::now()))
        .fetch_one(&mut *tx)
        .await
        .map_err(Error::storage)?;
        tx.commit().await.map_err(Error::storage)?;
        Ok(seq as u64)
    }

    async fn read(&self, run_id: RunId, from_seq: u64) -> Result<Vec<Event>> {
        let rows = sqlx::query_as::<_, RawEvent>(
            "SELECT seq, kind, payload, at FROM events WHERE run_id=$1 AND seq>$2 ORDER BY seq",
        )
        .bind(run_id.to_string())
        .bind(from_seq as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(Error::storage)?;
        rows.into_iter()
            .map(|r| {
                Ok(Event {
                    run_id,
                    seq: r.seq as u64,
                    kind: serde_json::from_str(&r.kind)?,
                    payload: serde_json::from_str(&r.payload)?,
                    at: dt(r.at),
                })
            })
            .collect()
    }
}

#[derive(FromRow)]
struct RawEvent {
    seq: i64,
    kind: String,
    payload: String,
    at: i64,
}

// ---- StepStore --------------------------------------------------------------

#[async_trait]
impl StepStore for PostgresStore {
    async fn lookup_step(&self, run_id: RunId, key: &str) -> Result<Option<StepRecord>> {
        let row = sqlx::query_as::<_, RawStep>(
            "SELECT status, output, error, attempts, updated_at FROM durable_steps \
             WHERE run_id=$1 AND key=$2",
        )
        .bind(run_id.to_string())
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(Error::storage)?;
        let Some(r) = row else { return Ok(None) };
        Ok(Some(StepRecord {
            run_id,
            key: StepKey(key.to_string()),
            status: serde_json::from_str(&r.status)?,
            output: r.output.map(|s| serde_json::from_str(&s)).transpose()?,
            error: r.error,
            attempts: r.attempts as u32,
            updated_at: dt(r.updated_at),
        }))
    }

    async fn record_step(&self, record: &StepRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO durable_steps (run_id, key, status, output, error, attempts, updated_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7) \
             ON CONFLICT (run_id, key) DO UPDATE SET \
               status=EXCLUDED.status, output=EXCLUDED.output, error=EXCLUDED.error, \
               attempts=EXCLUDED.attempts, updated_at=EXCLUDED.updated_at",
        )
        .bind(record.run_id.to_string())
        .bind(&record.key.0)
        .bind(to_json(&record.status)?)
        .bind(opt_value_to_json(&record.output))
        .bind(&record.error)
        .bind(record.attempts as i64)
        .bind(ms(record.updated_at))
        .execute(&self.pool)
        .await
        .map_err(Error::storage)?;
        Ok(())
    }
}

#[derive(FromRow)]
struct RawStep {
    status: String,
    output: Option<String>,
    error: Option<String>,
    attempts: i64,
    updated_at: i64,
}

// ---- Queue ------------------------------------------------------------------

#[async_trait]
impl Queue for PostgresStore {
    async fn enqueue(&self, run_id: RunId, ready_at: Option<DateTime<Utc>>) -> Result<()> {
        sqlx::query(
            "INSERT INTO queue (entry_id, run_id, ready_at, leased_until) VALUES ($1,$2,$3,NULL)",
        )
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(run_id.to_string())
        .bind(ms(ready_at.unwrap_or_else(Utc::now)))
        .execute(&self.pool)
        .await
        .map_err(Error::storage)?;
        Ok(())
    }

    async fn claim(&self, _worker_id: &str, lease_ttl: Duration) -> Result<Option<Lease<RunId>>> {
        let now = ms(Utc::now());
        let expires = now + lease_ttl.as_millis() as i64;
        // Claim one ready entry, skipping rows other workers hold a row-lock on.
        let row = sqlx::query_as::<_, RawClaim>(
            "UPDATE queue SET leased_until=$2 WHERE entry_id = ( \
                 SELECT entry_id FROM queue \
                 WHERE ready_at<=$1 AND (leased_until IS NULL OR leased_until<=$1) \
                 ORDER BY ready_at FOR UPDATE SKIP LOCKED LIMIT 1 \
             ) RETURNING entry_id, run_id",
        )
        .bind(now)
        .bind(expires)
        .fetch_optional(&self.pool)
        .await
        .map_err(Error::storage)?;
        let Some(r) = row else { return Ok(None) };
        Ok(Some(Lease {
            item: parse_run_id(&r.run_id)?,
            lease_id: r.entry_id.parse().map_err(Error::storage)?,
            expires_at: dt(expires),
        }))
    }

    async fn ack(&self, lease: &Lease<RunId>) -> Result<()> {
        sqlx::query("DELETE FROM queue WHERE entry_id=$1")
            .bind(lease.lease_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(Error::storage)?;
        Ok(())
    }
}

#[derive(FromRow)]
struct RawClaim {
    entry_id: String,
    run_id: String,
}

// ---- SignalStore ------------------------------------------------------------

#[async_trait]
impl SignalStore for PostgresStore {
    async fn deliver(&self, run_id: RunId, name: &str, payload: serde_json::Value) -> Result<()> {
        sqlx::query(
            "INSERT INTO signals (run_id, name, payload, created_at, consumed) \
             VALUES ($1,$2,$3,$4,FALSE)",
        )
        .bind(run_id.to_string())
        .bind(name)
        .bind(payload.to_string())
        .bind(ms(Utc::now()))
        .execute(&self.pool)
        .await
        .map_err(Error::storage)?;
        Ok(())
    }

    async fn take_signal(&self, run_id: RunId, name: &str) -> Result<Option<Signal>> {
        // Mark-and-return the earliest matching signal atomically; the row lock
        // (SKIP LOCKED) keeps concurrent consumers from taking the same one.
        let row = sqlx::query_as::<_, RawSignal>(
            "UPDATE signals SET consumed=TRUE WHERE id = ( \
                 SELECT id FROM signals \
                 WHERE run_id=$1 AND name=$2 AND consumed=FALSE \
                 ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED \
             ) RETURNING name, payload, created_at",
        )
        .bind(run_id.to_string())
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(Error::storage)?;
        row.map(|r| {
            Ok(Signal {
                run_id,
                name: r.name,
                payload: serde_json::from_str(&r.payload)?,
                created_at: dt(r.created_at),
                consumed: true,
            })
        })
        .transpose()
    }

    async fn has_unconsumed(&self, run_id: RunId, name: &str) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM signals WHERE run_id=$1 AND name=$2 AND consumed=FALSE)",
        )
        .bind(run_id.to_string())
        .bind(name)
        .fetch_one(&self.pool)
        .await
        .map_err(Error::storage)?;
        Ok(exists)
    }
}

#[derive(FromRow)]
struct RawSignal {
    name: String,
    payload: String,
    created_at: i64,
}

// ---- ResourceStore ----------------------------------------------------------

#[async_trait]
impl ResourceStore for PostgresStore {
    async fn lease_resource(&self, resource: &Resource) -> Result<()> {
        sqlx::query(
            "INSERT INTO resources (id, run_id, kind, status, metadata, lease_expires_at, created_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(resource.id.to_string())
        .bind(resource.run_id.to_string())
        .bind(to_json(&resource.kind)?)
        .bind(to_json(&resource.status)?)
        .bind(resource.metadata.to_string())
        .bind(resource.lease_expires_at.map(ms))
        .bind(ms(resource.created_at))
        .execute(&self.pool)
        .await
        .map_err(Error::storage)?;
        Ok(())
    }

    async fn heartbeat(&self, id: ResourceId, expires_at: DateTime<Utc>) -> Result<()> {
        sqlx::query("UPDATE resources SET lease_expires_at=$2 WHERE id=$1")
            .bind(id.to_string())
            .bind(ms(expires_at))
            .execute(&self.pool)
            .await
            .map_err(Error::storage)?;
        Ok(())
    }

    async fn release_resource(&self, id: ResourceId) -> Result<()> {
        sqlx::query("UPDATE resources SET status=$2 WHERE id=$1")
            .bind(id.to_string())
            .bind(to_json(&ResourceStatus::Released)?)
            .execute(&self.pool)
            .await
            .map_err(Error::storage)?;
        Ok(())
    }

    async fn list_resources(&self, run_id: RunId) -> Result<Vec<Resource>> {
        let rows = sqlx::query_as::<_, RawResource>(
            "SELECT id, kind, status, metadata, lease_expires_at, created_at \
             FROM resources WHERE run_id=$1 ORDER BY created_at",
        )
        .bind(run_id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(Error::storage)?;
        rows.into_iter()
            .map(|r| {
                Ok(Resource {
                    id: r.id.parse::<ResourceId>().map_err(Error::storage)?,
                    run_id,
                    kind: serde_json::from_str(&r.kind)?,
                    status: serde_json::from_str(&r.status)?,
                    metadata: serde_json::from_str(&r.metadata)?,
                    lease_expires_at: r.lease_expires_at.map(dt),
                    created_at: dt(r.created_at),
                })
            })
            .collect()
    }
}

#[derive(FromRow)]
struct RawResource {
    id: String,
    kind: String,
    status: String,
    metadata: String,
    lease_expires_at: Option<i64>,
    created_at: i64,
}

// ---- ArtifactStore ----------------------------------------------------------

#[async_trait]
impl ArtifactStore for PostgresStore {
    async fn put_artifact(&self, artifact: &Artifact) -> Result<()> {
        sqlx::query(
            "INSERT INTO artifacts (id, run_id, kind, uri, metadata, created_at) \
             VALUES ($1,$2,$3,$4,$5,$6)",
        )
        .bind(artifact.id.to_string())
        .bind(artifact.run_id.to_string())
        .bind(&artifact.kind)
        .bind(&artifact.uri)
        .bind(artifact.metadata.to_string())
        .bind(ms(artifact.created_at))
        .execute(&self.pool)
        .await
        .map_err(Error::storage)?;
        Ok(())
    }

    async fn list_artifacts(&self, run_id: RunId) -> Result<Vec<Artifact>> {
        let rows = sqlx::query_as::<_, RawArtifact>(
            "SELECT id, kind, uri, metadata, created_at FROM artifacts \
             WHERE run_id=$1 ORDER BY created_at",
        )
        .bind(run_id.to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(Error::storage)?;
        rows.into_iter()
            .map(|r| {
                Ok(Artifact {
                    id: r.id.parse::<ArtifactId>().map_err(Error::storage)?,
                    run_id,
                    kind: r.kind,
                    uri: r.uri,
                    metadata: serde_json::from_str(&r.metadata)?,
                    created_at: dt(r.created_at),
                })
            })
            .collect()
    }
}

#[derive(FromRow)]
struct RawArtifact {
    id: String,
    kind: String,
    uri: String,
    metadata: String,
    created_at: i64,
}

// ---- Clock ------------------------------------------------------------------

impl Clock for PostgresStore {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
