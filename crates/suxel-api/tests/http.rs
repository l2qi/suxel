// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end HTTP tests for the Suxel API router (via `tower::oneshot`).

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use suxel_core::{Engine, InMemoryBackend, RunSpec};
use tower::ServiceExt;

fn app() -> axum::Router {
    let engine = Arc::new(Engine::new(Arc::new(InMemoryBackend::new())));
    suxel_api::router(engine)
}

async fn json_body(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn create_then_fetch_a_run() {
    let app = app();

    let create = Request::builder()
        .method("POST")
        .uri("/runs")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({ "agent_type": "demo", "goal": "do a thing" }).to_string(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(create).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    let run_id = body["run_id"].as_str().unwrap().to_string();

    let get = Request::builder()
        .method("GET")
        .uri(format!("/runs/{run_id}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(get).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let run = json_body(resp).await;
    assert_eq!(run["agent_type"], "demo");
    assert_eq!(run["goal"], "do a thing");
    assert_eq!(run["status"], "pending");

    // The created run has a RunCreated event in its resumable log.
    let events = Request::builder()
        .method("GET")
        .uri(format!("/runs/{run_id}/events"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(events).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let events = json_body(resp).await;
    assert_eq!(events[0]["kind"], "run_created");
}

#[tokio::test]
async fn unknown_run_is_404_and_bad_id_is_400() {
    let app = app();

    let missing = Request::builder()
        .method("GET")
        .uri("/runs/0192f000-0000-7000-8000-000000000000")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(missing).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let bad = Request::builder()
        .method("GET")
        .uri("/runs/not-a-uuid")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(bad).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn streams_events_as_sse_until_terminal() {
    // No driver registered → the run reaches a terminal (failed) state when
    // processed, so the SSE stream replays its events and then closes.
    let engine = Arc::new(Engine::new(Arc::new(InMemoryBackend::new())));
    let id = engine
        .create_run(RunSpec {
            agent_type: "x".into(),
            goal: "g".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle("w").await.unwrap();

    let app = suxel_api::router(engine.clone());
    let req = Request::builder()
        .method("GET")
        .uri(format!("/runs/{id}/stream"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(ct.starts_with("text/event-stream"), "content-type was {ct}");

    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        body.contains("run_created"),
        "missing run_created in: {body}"
    );
    assert!(body.contains("done"), "missing done sentinel in: {body}");
}
