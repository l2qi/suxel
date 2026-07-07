// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! The agent-facing bridge: Sweet's [`CommandRunner`] + [`Filesystem`] over an
//! E2B sandbox's in-sandbox `envd` service.
//!
//! [`E2bProvider`] manages the sandbox *lifecycle* (create /
//! keepalive / kill) on E2B's control plane. This module is the other half — it
//! lets a Sweet agent/tool actually run commands and read/write files *inside* a
//! leased sandbox, by talking to `envd` (the daemon on port 49983) over its
//! Connect-RPC + HTTP file API. Hand the resulting [`E2bSandbox`] to Sweet as the
//! `Arc<dyn Sandbox>` and unchanged Sweet tools execute against the cloud microVM.
//!
//! Wire protocol (all reverse-engineered from E2B's `envd` source + protos):
//! - **Host**: `https://{ENVD_PORT}-{sandbox_id}.{domain}`.
//! - **Auth**: the run-as user is the HTTP Basic-Auth *username* (password
//!   ignored); a secured `envd` additionally requires an `X-Access-Token` header.
//! - **Filesystem metadata** (`Stat`/`MakeDir`/`Move`/`ListDir`/`Remove`): Connect
//!   unary JSON at `/filesystem.Filesystem/{Method}`.
//! - **File bytes**: there is no read/write RPC — content moves over the HTTP
//!   `GET`/`POST /files?path=&username=` endpoint (`POST` is multipart, field `file`).
//! - **Exec** (`/process.Process/Start`): a Connect *server-streaming* RPC whose
//!   enveloped frames carry `start` → `data{stdout|stderr}` → `end{exitCode}`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use suxel_core::driver::RunContext;
use suxel_core::{provision_sandbox, sandbox_resource, Error, Result as SuxelResult};
use sweet_core::sandbox::{
    CommandOutput, CommandRunner, DirEntry, FileMetadata, Filesystem, Sandbox, SandboxError,
};

use crate::E2bProvider;

/// The port `envd` listens on inside every E2B sandbox.
pub const ENVD_PORT: u16 = 49983;
/// Default base domain serving sandbox traffic.
pub const DEFAULT_DOMAIN: &str = "e2b.app";
/// Default sandbox user that commands run as and that owns written files.
pub const DEFAULT_USER: &str = "user";

/// How to reach a sandbox's `envd`. Defaults match E2B prod.
pub struct ConnectOptions {
    /// Base domain (the `domain` field returned by sandbox create; default `e2b.app`).
    pub domain: String,
    /// Run-as user inside the sandbox.
    pub user: String,
    /// `envd` access token, required only for a *secured* sandbox (the
    /// `envdAccessToken` field from sandbox create). `None` for an unsecured one.
    pub access_token: Option<String>,
    /// Reuse a preconfigured HTTP client (connection pooling). `None` builds one.
    pub http: Option<reqwest::Client>,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            domain: DEFAULT_DOMAIN.to_string(),
            user: DEFAULT_USER.to_string(),
            access_token: None,
            http: None,
        }
    }
}

/// A Sweet [`Sandbox`] backed by a live E2B sandbox's `envd`. Cheap to clone
/// (the HTTP client is internally reference-counted).
#[derive(Clone)]
pub struct E2bSandbox {
    http: reqwest::Client,
    /// `https://{port}-{id}.{domain}`, no trailing slash.
    host_base: String,
    user: String,
    access_token: Option<String>,
}

impl E2bSandbox {
    /// Connect to `sandbox_id`'s `envd` using the standard E2B host layout.
    pub fn connect(sandbox_id: &str, opts: ConnectOptions) -> Self {
        let host_base = format!("https://{ENVD_PORT}-{sandbox_id}.{}", opts.domain);
        Self::with_host_base(host_base, opts)
    }

    /// Construct against an explicit `envd` base URL (no `/` suffix). Used by the
    /// `connect` constructor and by tests pointing at a mock server.
    pub fn with_host_base(host_base: impl Into<String>, opts: ConnectOptions) -> Self {
        Self {
            http: opts.http.unwrap_or_default(),
            host_base: host_base.into(),
            user: opts.user,
            access_token: opts.access_token,
        }
    }

    /// Rebuild the bridge from a leased `CodeSandbox` resource's metadata — the
    /// re-attach path. The metadata is what [`E2bProvider`]
    /// persisted (`sandbox_id` + `domain` + `access_token`). `None` if there is no
    /// `sandbox_id`. Pass the provider's `http_client()` to reuse its pool.
    pub fn from_resource_metadata(
        metadata: &serde_json::Value,
        http: Option<reqwest::Client>,
    ) -> Option<Self> {
        let id = metadata.get("sandbox_id").and_then(|v| v.as_str())?;
        let domain = metadata
            .get("domain")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_DOMAIN)
            .to_string();
        let access_token = metadata
            .get("access_token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        Some(Self::connect(
            id,
            ConnectOptions {
                domain,
                access_token,
                http,
                ..Default::default()
            },
        ))
    }

    /// The `/files` URL for `path`, with `path` + `username` percent-encoded.
    fn files_url(&self, path: &Path) -> Result<reqwest::Url, SandboxError> {
        reqwest::Url::parse_with_params(
            &format!("{}/files", self.host_base),
            &[
                ("path", path_str(path).as_str()),
                ("username", self.user.as_str()),
            ],
        )
        .map_err(|e| backend(format!("building files url: {e}")))
    }

    /// Attach Basic-auth user + optional access token to an outgoing request.
    fn authed(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let rb = rb.basic_auth(&self.user, Some(""));
        match &self.access_token {
            Some(t) => rb.header("X-Access-Token", t),
            None => rb,
        }
    }

    /// A Connect unary JSON call to the Filesystem service. Returns the decoded
    /// response object, or a backend error carrying `envd`'s message.
    async fn fs_rpc<R: Serialize>(
        &self,
        method: &str,
        req: &R,
    ) -> Result<serde_json::Value, SandboxError> {
        let url = format!("{}/filesystem.Filesystem/{method}", self.host_base);
        let resp = self
            .authed(self.http.post(&url).header("Connect-Protocol-Version", "1"))
            .json(req)
            .send()
            .await
            .map_err(|e| backend(format!("{method}: {e}")))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| backend(format!("{method}: reading body: {e}")))?;
        if !status.is_success() {
            return Err(backend(connect_error_message(&bytes, status)));
        }
        serde_json::from_slice(&bytes).map_err(|e| backend(format!("{method}: decode: {e}")))
    }
}

// ---------------------------------------------------------------------------
// CommandRunner — exec via the Process.Start server-streaming RPC
// ---------------------------------------------------------------------------

#[async_trait]
impl CommandRunner for E2bSandbox {
    async fn run(
        &self,
        command: &str,
        cwd: Option<&Path>,
        env: Option<&HashMap<String, String>>,
    ) -> Result<CommandOutput, SandboxError> {
        let req = StartRequest {
            process: ProcessConfig {
                // Match E2B's own default: run the command through a login shell
                // so PATH and profile env are as a user would expect.
                cmd: "/bin/bash".to_string(),
                args: vec!["-l".to_string(), "-c".to_string(), command.to_string()],
                envs: env.cloned().unwrap_or_default(),
                cwd: cwd.map(|p| p.to_string_lossy().into_owned()),
            },
            // We never feed stdin for a run-to-completion command.
            stdin: false,
        };
        let body = encode_frame(
            &serde_json::to_vec(&req).map_err(|e| backend(format!("encode start: {e}")))?,
        );

        let url = format!("{}/process.Process/Start", self.host_base);
        let resp = self
            .authed(
                self.http
                    .post(&url)
                    .header("Content-Type", "application/connect+json")
                    .header("Connect-Protocol-Version", "1"),
            )
            .body(body)
            .send()
            .await
            .map_err(|e| backend(format!("process start: {e}")))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| backend(format!("process start: reading stream: {e}")))?;
        if !status.is_success() {
            return Err(backend(connect_error_message(&bytes, status)));
        }

        let mut stdout: Vec<u8> = Vec::new();
        let mut stderr: Vec<u8> = Vec::new();
        let mut exit_code: Option<i32> = None;

        for (flags, payload) in decode_frames(&bytes)? {
            if flags & FLAG_END_STREAM != 0 {
                // End-of-stream envelope: `{}` on success, else `{"error": {...}}`.
                if let Some(msg) = end_stream_error(payload) {
                    return Err(backend(msg));
                }
                break;
            }
            let resp: StartResponse = serde_json::from_slice(payload)
                .map_err(|e| backend(format!("decode process event: {e}")))?;
            let Some(event) = resp.event else { continue };
            if let Some(data) = event.data {
                if let Some(s) = data.stdout {
                    stdout.extend_from_slice(&b64(&s)?);
                }
                if let Some(s) = data.stderr {
                    stderr.extend_from_slice(&b64(&s)?);
                }
            }
            if let Some(end) = event.end {
                if let Some(err) = end.error {
                    return Err(backend(format!("process error: {err}")));
                }
                exit_code = end.exit_code;
            }
        }

        Ok(CommandOutput {
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
            // A stream that ended without an `end` event is a protocol fault; -1
            // is the conventional "no exit status captured" sentinel.
            exit_code: exit_code.unwrap_or(-1),
        })
    }
}

// ---------------------------------------------------------------------------
// Filesystem — metadata via Connect RPCs, bytes via the HTTP /files endpoint
// ---------------------------------------------------------------------------

#[async_trait]
impl Filesystem for E2bSandbox {
    async fn read(&self, path: &Path) -> Result<Vec<u8>, SandboxError> {
        let url = self.files_url(path)?;
        let resp = self
            .authed(self.http.get(url))
            .send()
            .await
            .map_err(|e| backend(format!("read {}: {e}", path.display())))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(backend(format!("no such file: {}", path.display())));
        }
        if !resp.status().is_success() {
            return Err(backend(format!(
                "read {}: HTTP {}",
                path.display(),
                resp.status()
            )));
        }
        Ok(resp
            .bytes()
            .await
            .map_err(|e| backend(format!("read {}: {e}", path.display())))?
            .to_vec())
    }

    async fn read_to_string(&self, path: &Path) -> Result<String, SandboxError> {
        let bytes = self.read(path).await?;
        String::from_utf8(bytes)
            .map_err(|e| backend(format!("{} is not utf-8: {e}", path.display())))
    }

    async fn write(&self, path: &Path, content: &[u8]) -> Result<(), SandboxError> {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".to_string());
        let part = reqwest::multipart::Part::bytes(content.to_vec()).file_name(name);
        let form = reqwest::multipart::Form::new().part("file", part);
        let url = self.files_url(path)?;
        let resp = self
            .authed(self.http.post(url))
            .multipart(form)
            .send()
            .await
            .map_err(|e| backend(format!("write {}: {e}", path.display())))?;
        if !resp.status().is_success() {
            return Err(backend(format!(
                "write {}: HTTP {}",
                path.display(),
                resp.status()
            )));
        }
        Ok(())
    }

    async fn metadata(&self, path: &Path) -> Result<FileMetadata, SandboxError> {
        let v = self
            .fs_rpc(
                "Stat",
                &PathReq {
                    path: path_str(path),
                },
            )
            .await?;
        let entry: EntryInfo = serde_json::from_value(v.get("entry").cloned().unwrap_or_default())
            .map_err(|e| backend(format!("stat {}: {e}", path.display())))?;
        Ok(entry.into_metadata())
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<DirEntry>, SandboxError> {
        let v = self
            .fs_rpc(
                "ListDir",
                &ListDirReq {
                    path: path_str(path),
                    depth: 1,
                },
            )
            .await?;
        let entries: Vec<EntryInfo> =
            serde_json::from_value(v.get("entries").cloned().unwrap_or(serde_json::json!([])))
                .map_err(|e| backend(format!("list_dir {}: {e}", path.display())))?;
        Ok(entries.into_iter().map(EntryInfo::into_dir_entry).collect())
    }

    async fn create_dir_all(&self, path: &Path) -> Result<(), SandboxError> {
        self.fs_rpc(
            "MakeDir",
            &PathReq {
                path: path_str(path),
            },
        )
        .await
        .map(|_| ())
    }

    async fn remove_file(&self, path: &Path) -> Result<(), SandboxError> {
        self.fs_rpc(
            "Remove",
            &PathReq {
                path: path_str(path),
            },
        )
        .await
        .map(|_| ())
    }

    async fn remove_dir_all(&self, path: &Path) -> Result<(), SandboxError> {
        // envd's Remove deletes recursively, so it covers both file and dir.
        self.fs_rpc(
            "Remove",
            &PathReq {
                path: path_str(path),
            },
        )
        .await
        .map(|_| ())
    }

    async fn rename(&self, src: &Path, dst: &Path) -> Result<(), SandboxError> {
        self.fs_rpc(
            "Move",
            &MoveReq {
                source: path_str(src),
                destination: path_str(dst),
            },
        )
        .await
        .map(|_| ())
    }

    async fn exists(&self, path: &Path) -> bool {
        self.metadata(path).await.is_ok()
    }
}

impl Sandbox for E2bSandbox {
    fn runner(&self) -> Arc<dyn CommandRunner> {
        Arc::new(self.clone())
    }
    fn fs(&self) -> Arc<dyn Filesystem> {
        Arc::new(self.clone())
    }
}

/// Provision (or, after a crash, re-attach to) an E2B sandbox for the current run
/// and return the connected bridge, ready to hand to a Sweet agent as the
/// `Arc<dyn Sandbox>`.
///
/// This is the durable entry point: [`provision_sandbox`] creates the VM at most
/// once (memoized) and leases it; on resume it re-attaches by id. Either way the
/// bridge is rebuilt from the lease's persisted connection metadata, reusing the
/// provider's HTTP pool. Pair with [`reap_sandboxes`](suxel_core::reap_sandboxes)
/// on completion/cancellation so the VM is never leaked.
pub async fn provision_e2b_sandbox(
    ctx: &mut RunContext,
    provider: &E2bProvider,
) -> SuxelResult<E2bSandbox> {
    provision_sandbox(ctx, provider).await?;
    let resource = sandbox_resource(ctx)
        .await?
        .ok_or_else(|| Error::Driver("sandbox lease missing after provision".to_string()))?;
    E2bSandbox::from_resource_metadata(&resource.metadata, Some(provider.http_client()))
        .ok_or_else(|| Error::Driver("leased sandbox metadata has no sandbox_id".to_string()))
}

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct PathReq {
    path: String,
}

#[derive(Serialize)]
struct ListDirReq {
    path: String,
    depth: u32,
}

#[derive(Serialize)]
struct MoveReq {
    source: String,
    destination: String,
}

#[derive(Serialize)]
struct StartRequest {
    process: ProcessConfig,
    stdin: bool,
}

#[derive(Serialize)]
struct ProcessConfig {
    cmd: String,
    args: Vec<String>,
    envs: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
}

/// `envd`'s `EntryInfo` as protojson emits it (camelCase fields; `size` is a
/// proto `int64` and therefore a JSON *string*; enums are their name strings).
#[derive(Deserialize, Default)]
struct EntryInfo {
    #[serde(default)]
    name: String,
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    path: String,
    #[serde(default, deserialize_with = "de_i64")]
    size: i64,
    #[serde(default)]
    mode: u32,
    #[serde(rename = "modifiedTime", default)]
    modified_time: Option<String>,
}

impl EntryInfo {
    fn into_metadata(self) -> FileMetadata {
        FileMetadata {
            size: self.size.max(0) as u64,
            is_dir: self.kind == "FILE_TYPE_DIRECTORY",
            is_symlink: self.kind == "FILE_TYPE_SYMLINK",
            modified: parse_rfc3339(self.modified_time.as_deref()),
            created: None,
            #[cfg(unix)]
            unix_permissions: Some(self.mode & 0o7777),
        }
    }

    fn into_dir_entry(self) -> DirEntry {
        let path = PathBuf::from(&self.path);
        let name = self.name.clone();
        DirEntry {
            name,
            path,
            metadata: self.into_metadata(),
        }
    }
}

#[derive(Deserialize)]
struct StartResponse {
    event: Option<ProcessEvent>,
}

#[derive(Deserialize)]
struct ProcessEvent {
    data: Option<DataEvent>,
    end: Option<EndEvent>,
}

#[derive(Deserialize)]
struct DataEvent {
    stdout: Option<String>,
    stderr: Option<String>,
}

#[derive(Deserialize)]
struct EndEvent {
    #[serde(rename = "exitCode")]
    exit_code: Option<i32>,
    error: Option<String>,
}

// ---------------------------------------------------------------------------
// Connect envelope framing + small helpers
// ---------------------------------------------------------------------------

/// Connect end-of-stream envelope flag.
const FLAG_END_STREAM: u8 = 0b0000_0010;

/// Wrap a payload in a single Connect envelope: `[flags:u8][len:u32 BE][payload]`.
fn encode_frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(0); // flags: not compressed, not end-of-stream
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Split a Connect stream body into `(flags, payload)` frames.
fn decode_frames(buf: &[u8]) -> Result<Vec<(u8, &[u8])>, SandboxError> {
    let mut frames = Vec::new();
    let mut i = 0;
    while i + 5 <= buf.len() {
        let flags = buf[i];
        let len = u32::from_be_bytes([buf[i + 1], buf[i + 2], buf[i + 3], buf[i + 4]]) as usize;
        i += 5;
        if i + len > buf.len() {
            return Err(backend("truncated Connect envelope".to_string()));
        }
        frames.push((flags, &buf[i..i + len]));
        i += len;
    }
    Ok(frames)
}

/// Extract an error message from a Connect end-of-stream payload, if any.
fn end_stream_error(payload: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let err = v.get("error")?;
    let code = err.get("code").and_then(|c| c.as_str()).unwrap_or("error");
    let msg = err.get("message").and_then(|m| m.as_str()).unwrap_or("");
    Some(format!("process stream error [{code}]: {msg}"))
}

/// Extract a Connect *unary* error message from a non-2xx body.
fn connect_error_message(bytes: &[u8], status: reqwest::StatusCode) -> String {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|v| {
            let code = v.get("code").and_then(|c| c.as_str());
            let msg = v.get("message").and_then(|m| m.as_str());
            match (code, msg) {
                (Some(c), Some(m)) => Some(format!("envd error [{c}]: {m}")),
                (None, Some(m)) => Some(m.to_string()),
                _ => None,
            }
        })
        .unwrap_or_else(|| format!("HTTP {status}"))
}

fn b64(s: &str) -> Result<Vec<u8>, SandboxError> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(s))
        .map_err(|e| backend(format!("invalid base64 in process output: {e}")))
}

fn backend(msg: String) -> SandboxError {
    SandboxError::Backend(msg)
}

fn path_str(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn parse_rfc3339(s: Option<&str>) -> Option<SystemTime> {
    let dt = chrono::DateTime::parse_from_rfc3339(s?).ok()?;
    let secs = dt.timestamp();
    if secs < 0 {
        return None;
    }
    Some(UNIX_EPOCH + Duration::new(secs as u64, dt.timestamp_subsec_nanos()))
}

/// Accept a proto `int64` whether protojson emitted it as a number or a string.
fn de_i64<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    struct V;
    impl de::Visitor<'_> for V {
        type Value = i64;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an integer or its string form")
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<i64, E> {
            Ok(v)
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<i64, E> {
            Ok(v as i64)
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<i64, E> {
            v.parse().map_err(de::Error::custom)
        }
    }
    d.deserialize_any(V)
}
