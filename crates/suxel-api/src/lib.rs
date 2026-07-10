// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! HTTP surface for the Suxel runtime: a [`Router`] factory the host mounts.
//!
//! `suxel-api` owns no auth and no user model — the host supplies
//! those and stamps tenancy onto requests. Mount it under your own middleware:
//!
//! ```no_run
//! use std::sync::Arc;
//! use suxel_core::{Engine, InMemoryBackend};
//! # async fn demo() {
//! let engine = Arc::new(Engine::new(Arc::new(InMemoryBackend::new())));
//! let app = axum::Router::new().nest("/api", suxel_api::router(engine));
//! # let _ = app;
//! # }
//! ```
//!
//! The event endpoint is **cursor-based and resumable**: `GET
//! /runs/{id}/events?from={seq}` returns events after `seq`, so a client that
//! reconnects resumes exactly where it left off (the same durable log backs both
//! live progress and audit).

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use suxel_core::error::Error;
use suxel_core::{ApprovalResolution, Budget, Engine, RunId, RunSpec};

/// Shared handler state.
#[derive(Clone)]
struct AppState {
    engine: Arc<Engine>,
}

/// Build the Suxel HTTP router over an [`Engine`].
pub fn router(engine: Arc<Engine>) -> Router {
    Router::new()
        .route("/runs", post(create_run))
        .route("/runs/{id}", get(get_run))
        .route("/runs/{id}/children", get(list_children))
        .route("/runs/{id}/events", get(get_events))
        .route("/runs/{id}/stream", get(stream_events))
        .route("/runs/{id}/artifacts", get(list_artifacts))
        .route("/runs/{id}/signal", post(signal))
        .route("/runs/{id}/approve", post(approve))
        .route("/runs/{id}/cancel", post(cancel))
        .with_state(AppState { engine })
}

// ---- error mapping ----------------------------------------------------------

/// Maps runtime errors to HTTP status codes.
enum ApiError {
    NotFound(String),
    BadRequest(String),
    Internal(String),
}

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        match e {
            Error::RunNotFound(_) => ApiError::NotFound(e.to_string()),
            other => ApiError::Internal(other.to_string()),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, msg) = match self {
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        (code, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}

fn parse_id(s: &str) -> Result<RunId, ApiError> {
    s.parse::<RunId>()
        .map_err(|e| ApiError::BadRequest(format!("invalid run id: {e}")))
}

// ---- request/response DTOs --------------------------------------------------

#[derive(Deserialize)]
struct CreateRunRequest {
    agent_type: String,
    goal: String,
    #[serde(default)]
    input: Option<Value>,
    #[serde(default)]
    session_ref: Option<String>,
    #[serde(default)]
    budget: Option<Budget>,
    #[serde(default)]
    parent_id: Option<String>,
}

#[derive(Serialize)]
struct CreateRunResponse {
    run_id: String,
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(default)]
    from: Option<u64>,
}

#[derive(Deserialize)]
struct SignalRequest {
    name: String,
    #[serde(default)]
    payload: Value,
}

#[derive(Deserialize)]
struct ApproveRequest {
    #[serde(flatten)]
    resolution: ApprovalResolution,
}

// ---- handlers ---------------------------------------------------------------

async fn create_run(
    State(state): State<AppState>,
    Json(req): Json<CreateRunRequest>,
) -> Result<Json<CreateRunResponse>, ApiError> {
    let parent_id = match req.parent_id {
        Some(p) => Some(parse_id(&p)?),
        None => None,
    };
    let id = state
        .engine
        .create_run(RunSpec {
            agent_type: req.agent_type,
            goal: req.goal,
            input: req.input,
            session_ref: req.session_ref,
            budget: req.budget.unwrap_or_default(),
            parent_id,
        })
        .await?;
    Ok(Json(CreateRunResponse {
        run_id: id.to_string(),
    }))
}

async fn get_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<suxel_core::Run>, ApiError> {
    let run = state.engine.get_run(parse_id(&id)?).await?;
    Ok(Json(run))
}

async fn list_children(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<suxel_core::Run>>, ApiError> {
    let kids = state.engine.backend().list_children(parse_id(&id)?).await?;
    Ok(Json(kids))
}

async fn get_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<EventsQuery>,
) -> Result<Json<Vec<suxel_core::Event>>, ApiError> {
    let events = state
        .engine
        .backend()
        .read(parse_id(&id)?, q.from.unwrap_or(0))
        .await?;
    Ok(Json(events))
}

/// Server-Sent Events tail of a run's event log: replays from `?from={seq}`,
/// then streams new events (poll-based, backend-agnostic) until the run reaches
/// a terminal state, ending with a `done` event. Resumable — a reconnecting
/// client passes the last seq it saw as `from`.
///
/// Each connected client independently polls the backend on a fixed interval
/// (below), so load is `N clients × 2 queries / interval`. That is fine for a UI
/// with a handful of listeners; at high fan-out a backend LISTEN/NOTIFY (Postgres)
/// or a shared in-process broadcast would scale better. Deliberate v1 tradeoff.
async fn stream_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<EventsQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    let run_id = parse_id(&id)?;
    state.engine.get_run(run_id).await?; // 404 if unknown
    let engine = state.engine.clone();
    let mut cursor = q.from.unwrap_or(0);

    // Close the stream with an `error` event after this many consecutive backend
    // failures (~5s at the poll interval) instead of spinning silently forever.
    const MAX_CONSECUTIVE_ERRORS: u32 = 20;

    let stream = async_stream::stream! {
        let mut errors = 0u32;
        loop {
            let mut had_error = false;
            match engine.backend().read(run_id, cursor).await {
                Ok(events) => {
                    for e in events {
                        cursor = e.seq;
                        if let Ok(ev) = SseEvent::default().json_data(&e) {
                            yield Ok(ev);
                        }
                    }
                }
                Err(_) => had_error = true,
            }
            match engine.get_run(run_id).await {
                Ok(run) if run.status.is_terminal() => {
                    // Final flush: the terminal event (e.g. RunCompleted) may
                    // have been committed after our last read. Read again so no
                    // event is lost before we close the stream.
                    if let Ok(events) = engine.backend().read(run_id, cursor).await {
                        for e in events {
                            if let Ok(ev) = SseEvent::default().json_data(&e) {
                                yield Ok(ev);
                            }
                        }
                    }
                    yield Ok(SseEvent::default().event("done").data("{}"));
                    break;
                }
                Ok(_) => {}
                Err(_) => had_error = true,
            }
            if had_error {
                errors += 1;
                if errors >= MAX_CONSECUTIVE_ERRORS {
                    yield Ok(SseEvent::default()
                        .event("error")
                        .data(r#"{"error":"stream backend unavailable"}"#));
                    break;
                }
            } else {
                errors = 0;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn list_artifacts(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<suxel_core::Artifact>>, ApiError> {
    let artifacts = state
        .engine
        .backend()
        .list_artifacts(parse_id(&id)?)
        .await?;
    Ok(Json(artifacts))
}

async fn signal(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<SignalRequest>,
) -> Result<Json<Value>, ApiError> {
    state
        .engine
        .signal(parse_id(&id)?, &req.name, req.payload)
        .await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

async fn approve(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<ApproveRequest>,
) -> Result<Json<Value>, ApiError> {
    state.engine.approve(parse_id(&id)?, req.resolution).await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    state.engine.cancel(parse_id(&id)?).await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}
