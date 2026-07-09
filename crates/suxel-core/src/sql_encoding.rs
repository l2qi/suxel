// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared SQL encoding for the storage backends.
//!
//! Every SQL store (`suxel-store-sqlite`, `suxel-store-postgres`) encodes the
//! same logical schema: domain values as JSON text, timestamps as epoch-millis
//! integers, and a fixed `runs` column order. These helpers hold that one
//! canonical encoding so the backends can't drift on it — each backend only owns
//! its own row-reading (rusqlite `Row` vs sqlx `FromRow`), then decodes through
//! [`run_from_row`].

use crate::error::{Error, Result};
use crate::ids::RunId;
use crate::run::Run;
use chrono::{DateTime, Utc};

/// Encode a timestamp as epoch milliseconds.
pub fn ms(dt: DateTime<Utc>) -> i64 {
    dt.timestamp_millis()
}

/// Decode an epoch-millisecond timestamp.
pub fn dt(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_default()
}

/// Serialize a value to its stored JSON text.
pub fn to_json<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(Error::from)
}

/// Serialize an optional JSON value to optional stored text.
pub fn opt_value_to_json(v: &Option<serde_json::Value>) -> Option<String> {
    v.as_ref().map(|v| v.to_string())
}

/// Parse a [`RunId`] from its stored string form.
pub fn parse_run_id(s: &str) -> Result<RunId> {
    s.parse::<RunId>().map_err(Error::storage)
}

/// The ordered `runs` columns, shared by every SQL backend's SELECT/INSERT.
pub const RUN_COLUMNS: &str = "id, parent_id, agent_type, goal, input, status, waiting, \
                               session_ref, budget, usage, output, error, created_at, updated_at";

/// The raw string/primitive column values of a `runs` row. A backend reads its
/// own row type into this, then converts to a domain [`Run`] via [`run_from_row`].
pub struct RunRow {
    pub id: String,
    pub parent_id: Option<String>,
    pub agent_type: String,
    pub goal: String,
    pub input: Option<String>,
    pub status: String,
    pub waiting: Option<String>,
    pub session_ref: Option<String>,
    pub budget: String,
    pub usage: String,
    pub output: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Decode a raw [`RunRow`] into a domain [`Run`].
pub fn run_from_row(r: RunRow) -> Result<Run> {
    Ok(Run {
        id: parse_run_id(&r.id)?,
        parent_id: r.parent_id.as_deref().map(parse_run_id).transpose()?,
        agent_type: r.agent_type,
        goal: r.goal,
        input: r.input.map(|s| serde_json::from_str(&s)).transpose()?,
        status: serde_json::from_str(&r.status)?,
        waiting: r.waiting.map(|s| serde_json::from_str(&s)).transpose()?,
        session_ref: r.session_ref,
        budget: serde_json::from_str(&r.budget)?,
        usage: serde_json::from_str(&r.usage)?,
        output: r.output.map(|s| serde_json::from_str(&s)).transpose()?,
        error: r.error,
        created_at: dt(r.created_at),
        updated_at: dt(r.updated_at),
    })
}
