// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Hermetic tests for the E2B provider's wire format (wiremock, no real key).

use suxel_core::SandboxProvider;
use suxel_sandbox_e2b::E2bProvider;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn create_keepalive_destroy_hit_the_right_endpoints() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/sandboxes"))
        .and(header("x-api-key", "test-key"))
        .and(body_partial_json(
            serde_json::json!({ "templateID": "base", "timeout": 600 }),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "sandboxID": "sb-abc123",
            "templateID": "base",
            "envdVersion": "0.1.0"
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/sandboxes/sb-abc123/timeout"))
        .and(header("x-api-key", "test-key"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/sandboxes/sb-abc123"))
        .and(header("x-api-key", "test-key"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let provider = E2bProvider::new("test-key")
        .with_base_url(server.uri())
        .with_timeout_secs(600);

    let handle = provider.create().await.unwrap();
    assert_eq!(handle.id, "sb-abc123");
    provider.keepalive(&handle.id).await.unwrap();
    provider.destroy(&handle.id).await.unwrap();
    // Mock `.expect(1)` assertions are verified on server drop.
}

#[tokio::test]
async fn destroy_tolerates_already_gone() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/sandboxes/missing"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let provider = E2bProvider::new("k").with_base_url(server.uri());
    provider.destroy("missing").await.unwrap();
}

#[tokio::test]
async fn create_surfaces_http_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/sandboxes"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;

    let provider = E2bProvider::new("bad").with_base_url(server.uri());
    let err = provider.create().await.unwrap_err();
    assert!(err.contains("401"), "error was: {err}");
}

/// Live smoke test against the real E2B control plane. Ignored by default —
/// run explicitly with `--ignored` and `E2B_API_KEY` in the environment:
///
/// ```text
/// cargo test -p suxel-sandbox-e2b --test e2b live_lifecycle -- --ignored --nocapture
/// ```
///
/// Exercises the full provider lifecycle (create → keepalive → destroy) so a
/// green run confirms the API key and the wire format end-to-end. The short
/// timeout and immediate destroy keep cost negligible.
#[tokio::test]
#[ignore = "hits the live E2B API; requires E2B_API_KEY"]
async fn live_lifecycle() {
    let provider = E2bProvider::from_env()
        .expect("E2B_API_KEY must be set to run the live test")
        .with_timeout_secs(60);

    let id = provider.create().await.expect("create a live sandbox").id;
    eprintln!("created sandbox: {id}");

    provider
        .keepalive(&id)
        .await
        .expect("refresh the sandbox timeout");
    eprintln!("kept sandbox alive: {id}");

    provider.destroy(&id).await.expect("destroy the sandbox");
    eprintln!("destroyed sandbox: {id}");
}
