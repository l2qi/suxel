// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! E2B cloud-sandbox provider — implements the Suxel
//! [`SandboxProvider`] lifecycle over E2B's control plane
//! (`https://api.e2b.app`): create, refresh-timeout (keepalive), and kill.
//!
//! Wired into the durable lifecycle, this lets a run provision an E2B sandbox as
//! a leased resource, re-attach to it by id after a crash, and reap it on
//! teardown so no paid VM leaks. The agent-facing execution/filesystem bridge —
//! Sweet's `CommandRunner`/`Filesystem` over the in-sandbox `envd` service — lives
//! in the [`bridge`] module ([`E2bSandbox`]); hand it to a Sweet agent as the
//! `Arc<dyn Sandbox>` and unchanged tools run inside the leased sandbox.
//!
//! ```no_run
//! use suxel_sandbox_e2b::{E2bProvider, E2bSandbox, ConnectOptions, DEFAULT_DOMAIN};
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = E2bProvider::from_env()?.with_template("base").with_timeout_secs(600);
//! let created = provider.create_sandbox().await?;
//! let sandbox = E2bSandbox::connect(
//!     &created.sandbox_id,
//!     ConnectOptions {
//!         domain: created.domain.unwrap_or_else(|| DEFAULT_DOMAIN.to_string()),
//!         access_token: created.envd_access_token,
//!         ..Default::default()
//!     },
//! );
//! # let _ = sandbox;
//! # Ok(()) }
//! ```

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use suxel_core::{SandboxHandle, SandboxProvider};

pub mod bridge;
pub use bridge::{
    provision_e2b_sandbox, ConnectOptions, E2bSandbox, DEFAULT_DOMAIN, DEFAULT_USER, ENVD_PORT,
};

pub mod computer;
pub use computer::{browser_tool, E2bComputerUse, DEFAULT_BROWSER_CMD};

/// Default E2B control-plane base URL.
pub const DEFAULT_BASE_URL: &str = "https://api.e2b.app";
/// Environment variable holding the E2B API key.
pub const DEFAULT_API_KEY_ENV: &str = "E2B_API_KEY";
/// Default sandbox template.
pub const DEFAULT_TEMPLATE: &str = "base";
/// Default sandbox timeout (seconds).
pub const DEFAULT_TIMEOUT_SECS: u64 = 300;

/// Errors constructing an [`E2bProvider`].
#[derive(Debug, thiserror::Error)]
pub enum E2bError {
    /// The API key environment variable is unset.
    #[error("missing {0} environment variable")]
    MissingApiKey(&'static str),
}

/// An E2B sandbox provider. Build with [`E2bProvider::new`] / [`E2bProvider::from_env`]
/// and the `with_*` builders.
pub struct E2bProvider {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    template_id: String,
    timeout_secs: u64,
}

impl E2bProvider {
    /// Construct from an explicit API key, with defaults for everything else.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: DEFAULT_BASE_URL.to_string(),
            api_key: api_key.into(),
            template_id: DEFAULT_TEMPLATE.to_string(),
            timeout_secs: DEFAULT_TIMEOUT_SECS,
        }
    }

    /// Construct from the `E2B_API_KEY` environment variable.
    pub fn from_env() -> Result<Self, E2bError> {
        let key = std::env::var(DEFAULT_API_KEY_ENV)
            .map_err(|_| E2bError::MissingApiKey(DEFAULT_API_KEY_ENV))?;
        Ok(Self::new(key))
    }

    /// Override the control-plane base URL (e.g. a self-hosted E2B or a test server).
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    /// Override the sandbox template id.
    pub fn with_template(mut self, template_id: impl Into<String>) -> Self {
        self.template_id = template_id.into();
        self
    }

    /// Override the sandbox timeout (seconds), used on create and keepalive.
    pub fn with_timeout_secs(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    /// Use a preconfigured HTTP client.
    pub fn with_http_client(mut self, client: reqwest::Client) -> Self {
        self.http = client;
        self
    }

    /// The provider's HTTP client (cloning is cheap — pooling is shared), so the
    /// bridge can reuse the same connection pool when re-attaching.
    pub fn http_client(&self) -> reqwest::Client {
        self.http.clone()
    }

    /// Create a sandbox and return its id plus the connection details
    /// ([`CreatedSandbox`]) needed to reach `envd`. [`SandboxProvider::create`]
    /// wraps this and keeps only the id (the durable lifecycle persists the id;
    /// re-attach wiring will persist the rest).
    pub async fn create_sandbox(&self) -> Result<CreatedSandbox, String> {
        let resp = self
            .http
            .post(format!("{}/sandboxes", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&CreateRequest {
                template_id: &self.template_id,
                timeout: self.timeout_secs,
            })
            .send()
            .await
            .map_err(|e| format!("e2b create request: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("e2b create failed: HTTP {}", resp.status()));
        }
        let body: CreateResponse = resp
            .json()
            .await
            .map_err(|e| format!("e2b create response: {e}"))?;
        Ok(CreatedSandbox {
            sandbox_id: body.sandbox_id,
            domain: body.domain,
            envd_access_token: body.envd_access_token,
        })
    }
}

#[derive(Serialize)]
struct CreateRequest<'a> {
    #[serde(rename = "templateID")]
    template_id: &'a str,
    timeout: u64,
}

#[derive(Deserialize)]
struct CreateResponse {
    #[serde(rename = "sandboxID")]
    sandbox_id: String,
    /// Base domain serving the sandbox's traffic (e.g. `e2b.app`). Optional in
    /// the API; falls back to the provider's configured default when absent.
    #[serde(default)]
    domain: Option<String>,
    /// Access token for a *secured* `envd`. Absent for an unsecured sandbox.
    #[serde(rename = "envdAccessToken", default)]
    envd_access_token: Option<String>,
}

/// A freshly created sandbox plus the details needed to reach its `envd` — feed
/// these into [`E2bSandbox::connect`](crate::E2bSandbox::connect) to run commands
/// and move files inside it.
#[derive(Debug, Clone)]
pub struct CreatedSandbox {
    /// The sandbox id (also the durable resource handle).
    pub sandbox_id: String,
    /// Base domain, if the API returned one.
    pub domain: Option<String>,
    /// `envd` access token, if the sandbox is secured.
    pub envd_access_token: Option<String>,
}

#[derive(Serialize)]
struct TimeoutRequest {
    timeout: u64,
}

#[async_trait]
impl SandboxProvider for E2bProvider {
    async fn create(&self) -> Result<SandboxHandle, String> {
        let created = self.create_sandbox().await?;
        // Persist the envd connection details on the lease so a recovering worker
        // rebuilds the bridge ([`E2bSandbox::from_resource_metadata`]) without a
        // new VM. `domain`/`access_token` may be null (unsecured / default domain).
        Ok(SandboxHandle::with_metadata(
            created.sandbox_id,
            serde_json::json!({
                "domain": created.domain,
                "access_token": created.envd_access_token,
            }),
        ))
    }

    async fn keepalive(&self, sandbox_id: &str) -> Result<(), String> {
        let resp = self
            .http
            .post(format!("{}/sandboxes/{sandbox_id}/timeout", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(&TimeoutRequest {
                timeout: self.timeout_secs,
            })
            .send()
            .await
            .map_err(|e| format!("e2b timeout request: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("e2b timeout failed: HTTP {}", resp.status()));
        }
        Ok(())
    }

    async fn destroy(&self, sandbox_id: &str) -> Result<(), String> {
        let resp = self
            .http
            .delete(format!("{}/sandboxes/{sandbox_id}", self.base_url))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .map_err(|e| format!("e2b kill request: {e}"))?;
        // A missing sandbox (already gone) is fine for a reaper.
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            return Err(format!("e2b kill failed: HTTP {}", resp.status()));
        }
        Ok(())
    }
}
