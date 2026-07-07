// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Hermetic tests for the envd bridge's wire format (wiremock, no real sandbox).
//!
//! These pin the exact request encoding and response decoding for the Connect
//! filesystem RPCs, the HTTP `/files` transfer, and the Connect server-streaming
//! `process.Process/Start` exec — so the bridge can be verified without a live
//! E2B sandbox. The `#[ignore]`d live test in `e2b.rs` covers true end-to-end.

use std::path::Path;

use suxel_core::SandboxProvider;
use suxel_sandbox_e2b::{ConnectOptions, E2bProvider, E2bSandbox, DEFAULT_DOMAIN};
use sweet_core::sandbox::{CommandRunner, Filesystem};
use wiremock::matchers::{body_string_contains, header, method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn sandbox(server: &MockServer) -> E2bSandbox {
    E2bSandbox::with_host_base(server.uri(), ConnectOptions::default())
}

/// Build a Connect envelope frame: `[flags][len: u32 BE][payload]`.
fn frame(flags: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![flags];
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

#[tokio::test]
async fn stat_maps_entry_to_metadata() {
    let server = MockServer::start().await;
    // protojson: camelCase fields, int64 `size` as a string, enum as its name.
    Mock::given(method("POST"))
        .and(path("/filesystem.Filesystem/Stat"))
        .and(header("connect-protocol-version", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "entry": {
                "name": "main.rs",
                "type": "FILE_TYPE_FILE",
                "path": "/home/user/main.rs",
                "size": "1234",
                "mode": 420,
                "permissions": "-rw-r--r--",
                "modifiedTime": "2026-06-28T10:00:00Z"
            }
        })))
        .mount(&server)
        .await;

    let meta = sandbox(&server)
        .metadata(Path::new("/home/user/main.rs"))
        .await
        .unwrap();
    assert_eq!(meta.size, 1234);
    assert!(!meta.is_dir);
    assert!(!meta.is_symlink);
    assert!(meta.modified.is_some());
    #[cfg(unix)]
    assert_eq!(meta.unix_permissions, Some(0o644));
}

#[tokio::test]
async fn list_dir_returns_entries() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/filesystem.Filesystem/ListDir"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "entries": [
                { "name": "src", "type": "FILE_TYPE_DIRECTORY", "path": "/app/src", "size": "0" },
                { "name": "README.md", "type": "FILE_TYPE_FILE", "path": "/app/README.md", "size": "42" }
            ]
        })))
        .mount(&server)
        .await;

    let entries = sandbox(&server).list_dir(Path::new("/app")).await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].name, "src");
    assert!(entries[0].metadata.is_dir);
    assert_eq!(entries[1].name, "README.md");
    assert_eq!(entries[1].metadata.size, 42);
}

#[tokio::test]
async fn make_dir_move_remove_are_unit_calls() {
    let server = MockServer::start().await;
    for m in ["MakeDir", "Move", "Remove"] {
        Mock::given(method("POST"))
            .and(path(format!("/filesystem.Filesystem/{m}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
    }
    let sb = sandbox(&server);
    sb.create_dir_all(Path::new("/app/new")).await.unwrap();
    sb.rename(Path::new("/app/a"), Path::new("/app/b"))
        .await
        .unwrap();
    sb.remove_file(Path::new("/app/b")).await.unwrap();
}

#[tokio::test]
async fn fs_rpc_surfaces_connect_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/filesystem.Filesystem/Stat"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "code": "not_found",
            "message": "path does not exist"
        })))
        .mount(&server)
        .await;

    let err = sandbox(&server)
        .metadata(Path::new("/nope"))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("path does not exist"), "got: {err}");
}

#[tokio::test]
async fn read_downloads_from_files_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/files"))
        .and(query_param("path", "/home/user/data.txt"))
        .and(query_param("username", "user"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello sandbox".to_vec()))
        .mount(&server)
        .await;

    let bytes = sandbox(&server)
        .read(Path::new("/home/user/data.txt"))
        .await
        .unwrap();
    assert_eq!(bytes, b"hello sandbox");
}

#[tokio::test]
async fn read_missing_is_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/files"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let err = sandbox(&server)
        .read(Path::new("/missing"))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no such file"), "got: {err}");
}

#[tokio::test]
async fn write_uploads_multipart_to_files_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/files"))
        .and(query_param("path", "/home/user/out.txt"))
        .and(query_param("username", "user"))
        // multipart part name is `file`; the bytes appear in the body.
        .and(body_string_contains("name=\"file\""))
        .and(body_string_contains("payload-bytes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            { "name": "out.txt", "type": "FILE_TYPE_FILE", "path": "/home/user/out.txt", "size": "13" }
        ])))
        .mount(&server)
        .await;

    sandbox(&server)
        .write(Path::new("/home/user/out.txt"), b"payload-bytes")
        .await
        .unwrap();
}

#[tokio::test]
async fn run_collects_streamed_stdout_stderr_and_exit_code() {
    let server = MockServer::start().await;
    // protojson bytes are base64; `aGk=` = "hi", `ZXJy` = "err".
    let mut body = Vec::new();
    body.extend(frame(0, br#"{"event":{"start":{"pid":42}}}"#));
    body.extend(frame(0, br#"{"event":{"data":{"stdout":"aGk="}}}"#));
    body.extend(frame(0, br#"{"event":{"data":{"stderr":"ZXJy"}}}"#));
    body.extend(frame(
        0,
        br#"{"event":{"end":{"exitCode":0,"exited":true,"status":"done"}}}"#,
    ));
    body.extend(frame(2, b"{}")); // end-of-stream

    Mock::given(method("POST"))
        .and(path("/process.Process/Start"))
        .and(header("content-type", "application/connect+json"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/connect+json")
                .set_body_bytes(body),
        )
        .mount(&server)
        .await;

    let out = sandbox(&server).run("echo hi", None, None).await.unwrap();
    assert_eq!(out.stdout, "hi");
    assert_eq!(out.stderr, "err");
    assert_eq!(out.exit_code, 0);
}

#[tokio::test]
async fn run_sends_command_through_login_bash() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/process.Process/Start"))
        // The StartRequest JSON (inside the envelope) carries the bash invocation.
        .and(body_string_contains("/bin/bash"))
        .and(body_string_contains("ls -la"))
        .respond_with(|_: &Request| {
            let mut body = Vec::new();
            body.extend(frame(
                0,
                br#"{"event":{"end":{"exitCode":7,"exited":true}}}"#,
            ));
            body.extend(frame(2, b"{}"));
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/connect+json")
                .set_body_bytes(body)
        })
        .mount(&server)
        .await;

    let out = sandbox(&server).run("ls -la", None, None).await.unwrap();
    assert_eq!(out.exit_code, 7);
}

#[tokio::test]
async fn run_surfaces_end_of_stream_error() {
    let server = MockServer::start().await;
    let mut body = Vec::new();
    body.extend(frame(
        2,
        br#"{"error":{"code":"internal","message":"boom"}}"#,
    ));
    Mock::given(method("POST"))
        .and(path("/process.Process/Start"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/connect+json")
                .set_body_bytes(body),
        )
        .mount(&server)
        .await;

    let err = sandbox(&server)
        .run("false", None, None)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("boom"), "got: {err}");
}

#[test]
fn from_resource_metadata_requires_a_sandbox_id() {
    // No id -> can't rebuild.
    assert!(E2bSandbox::from_resource_metadata(&serde_json::json!({}), None).is_none());
    // The metadata the provider persists (id + domain + token) rebuilds fine.
    assert!(E2bSandbox::from_resource_metadata(
        &serde_json::json!({ "sandbox_id": "sb-1", "domain": "e2b.app", "access_token": null }),
        None,
    )
    .is_some());
}

#[tokio::test]
async fn secured_sandbox_sends_access_token_and_run_as_user() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/filesystem.Filesystem/Stat"))
        .and(header("x-access-token", "secret"))
        // Basic auth username carries the run-as user; base64("user:") = "dXNlcjo=".
        .and(header("authorization", "Basic dXNlcjo="))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "entry": { "name": "x", "type": "FILE_TYPE_FILE", "path": "/x", "size": "0" }
        })))
        .mount(&server)
        .await;

    let sb = E2bSandbox::with_host_base(
        server.uri(),
        ConnectOptions {
            access_token: Some("secret".to_string()),
            ..Default::default()
        },
    );
    sb.metadata(Path::new("/x")).await.unwrap();
}

/// Live end-to-end against a real E2B sandbox: provision, then exercise the
/// bridge (exec with stdout/stderr/exit code + a file write/read/stat
/// round-trip), then reap. Ignored by default — run with `E2B_API_KEY` set:
///
/// ```text
/// cargo test -p suxel-sandbox-e2b --test bridge live_bridge -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "hits the live E2B API; requires E2B_API_KEY"]
async fn live_bridge() {
    let provider = E2bProvider::from_env()
        .expect("E2B_API_KEY must be set")
        .with_timeout_secs(120);
    let created = provider.create_sandbox().await.expect("create sandbox");
    eprintln!("sandbox: {}", created.sandbox_id);

    let sandbox = E2bSandbox::connect(
        &created.sandbox_id,
        ConnectOptions {
            domain: created.domain.unwrap_or_else(|| DEFAULT_DOMAIN.to_string()),
            access_token: created.envd_access_token,
            ..Default::default()
        },
    );

    // Exec: stdout, stderr, and a non-zero exit are all captured.
    let out = sandbox
        .run("echo out && echo problem >&2 && exit 3", None, None)
        .await
        .expect("run command");
    eprintln!(
        "stdout={:?} stderr={:?} exit={}",
        out.stdout, out.stderr, out.exit_code
    );
    assert_eq!(out.stdout.trim(), "out");
    assert!(out.stderr.contains("problem"));
    assert_eq!(out.exit_code, 3);

    // Files: write -> read round-trip + stat.
    let path = Path::new("/home/user/suxel-e2e.txt");
    sandbox.write(path, b"durable").await.expect("write file");
    let back = sandbox.read(path).await.expect("read file");
    assert_eq!(back, b"durable");
    let meta = sandbox.metadata(path).await.expect("stat file");
    assert_eq!(meta.size, 7);
    assert!(!meta.is_dir);

    provider
        .destroy(&created.sandbox_id)
        .await
        .expect("reap sandbox");
    eprintln!("destroyed: {}", created.sandbox_id);
}
