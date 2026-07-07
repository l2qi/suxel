// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Browser / computer-use over an E2B sandbox.
//!
//! Implements Sweet's [`ComputerUseProvider`] by driving an **in-sandbox browser
//! control CLI** (`browserctl`) through the bridge's [`CommandRunner`] — so a
//! Sweet agent gets the standard `computer` tool ([`computer_use_tool`]) and can
//! drive a headless Chromium running in the sandbox. The provider only needs a
//! `CommandRunner`, so it pairs with any sandbox (here, [`E2bSandbox`](crate::E2bSandbox)).
//!
//! ## Prerequisite (ops / sandbox template)
//!
//! The sandbox image must ship a headless browser plus a small `browserctl` CLI
//! (a thin Playwright/CDP wrapper) exposing this contract — the host stays
//! decoupled from the browser harness:
//!
//! - `browserctl observe [--screenshot]` → one JSON object:
//!   `{ "title": str?, "url": str?, "width": u32, "height": u32,
//!      "outline": str?, "screenshot_b64": str? }`.
//! - `browserctl <verb> <args…>` for actions: `navigate <url>`, `click <x> <y>
//!   [button]`, `dblclick <x> <y>`, `move <x> <y>`, `scroll <x> <y> <dx> <dy>`,
//!   `drag <x1> <y1> <x2> <y2>`, `type <text>`, `key <chord>`, `wait <ms>`.
//!   A zero exit is success; stdout is the human-readable detail.

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use serde::Deserialize;

use sweet_computer_use_core::{
    computer_use_tool, ActionOutcome, ComputerAction, ComputerObservation, ComputerUseError,
    ComputerUseProvider, CoordinateSpace, MouseButton, ObserveOptions, Screenshot, SharedProvider,
    Size, UiNode,
};
use sweet_core::sandbox::CommandRunner;
use sweet_core::ToolSpec;

/// Default in-sandbox browser-control command.
pub const DEFAULT_BROWSER_CMD: &str = "browserctl";

/// A Sweet computer-use backend that drives an in-sandbox browser via `browserctl`.
pub struct E2bComputerUse {
    runner: Arc<dyn CommandRunner>,
    cmd: String,
}

impl E2bComputerUse {
    /// Build over a sandbox command runner (e.g. `E2bSandbox::runner()`).
    pub fn new(runner: Arc<dyn CommandRunner>) -> Self {
        Self {
            runner,
            cmd: DEFAULT_BROWSER_CMD.to_string(),
        }
    }

    /// Override the in-sandbox control command (a template-provided wrapper).
    pub fn with_command(mut self, cmd: impl Into<String>) -> Self {
        self.cmd = cmd.into();
        self
    }

    async fn run(&self, args: &str) -> Result<String, ComputerUseError> {
        let out = self
            .runner
            .run(&format!("{} {args}", self.cmd), None, None)
            .await
            .map_err(|e| ComputerUseError::Platform(e.to_string()))?;
        if out.exit_code != 0 {
            return Err(ComputerUseError::Platform(format!(
                "browserctl {args} (exit {}): {}",
                out.exit_code, out.stderr
            )));
        }
        Ok(out.stdout)
    }
}

/// The JSON `browserctl observe` prints.
#[derive(Deserialize, Default)]
struct ObserveJson {
    title: Option<String>,
    url: Option<String>,
    #[serde(default)]
    width: u32,
    #[serde(default)]
    height: u32,
    outline: Option<String>,
    screenshot_b64: Option<String>,
}

#[async_trait]
impl ComputerUseProvider for E2bComputerUse {
    fn platform(&self) -> &'static str {
        "e2b-browser"
    }

    async fn observe(
        &self,
        opts: &ObserveOptions,
    ) -> Result<ComputerObservation, ComputerUseError> {
        let flag = if opts.include_screenshot {
            " --screenshot"
        } else {
            ""
        };
        let stdout = self.run(&format!("observe{flag}")).await?;
        let v: ObserveJson = serde_json::from_str(stdout.trim())
            .map_err(|e| ComputerUseError::Platform(format!("parsing observe output: {e}")))?;

        let screenshot = v.screenshot_b64.as_deref().and_then(|b64| {
            base64::engine::general_purpose::STANDARD
                .decode(b64)
                .ok()
                .map(|data| Screenshot {
                    data,
                    media_type: "image/png".to_string(),
                    path: None,
                    width: v.width,
                    height: v.height,
                })
        });

        // Model the page as a single accessibility node carrying the text outline.
        let accessibility_tree = if opts.include_tree {
            v.outline.map(|outline| UiNode {
                path: "0".to_string(),
                role: "WebArea".to_string(),
                title: v.url.clone(),
                label: None,
                value: Some(outline),
                identifier: None,
                frame: None,
                enabled: true,
                focused: true,
                actions: Vec::new(),
                children: Vec::new(),
            })
        } else {
            None
        };

        Ok(ComputerObservation {
            screen_size: Size {
                width: v.width as f64,
                height: v.height as f64,
            },
            active_app: Some("browser".to_string()),
            active_window_title: v.title,
            accessibility_tree,
            screenshot,
            ..Default::default()
        })
    }

    async fn act(&self, action: &ComputerAction) -> Result<ActionOutcome, ComputerUseError> {
        let args = match action {
            ComputerAction::Click { x, y, button } => {
                format!("click {} {} {}", px(*x), px(*y), button_name(*button))
            }
            ComputerAction::DoubleClick { x, y } => format!("dblclick {} {}", px(*x), px(*y)),
            ComputerAction::RightClick { x, y } => format!("click {} {} right", px(*x), px(*y)),
            ComputerAction::MoveCursor { x, y } => format!("move {} {}", px(*x), px(*y)),
            ComputerAction::Scroll { x, y, dx, dy } => {
                format!("scroll {} {} {} {}", px(*x), px(*y), px(*dx), px(*dy))
            }
            ComputerAction::Drag {
                from_x,
                from_y,
                to_x,
                to_y,
            } => format!(
                "drag {} {} {} {}",
                px(*from_x),
                px(*from_y),
                px(*to_x),
                px(*to_y)
            ),
            ComputerAction::TypeText { text } => format!("type {}", shell_quote(text)),
            ComputerAction::KeyChord { keys } => format!("key {}", shell_quote(&keys.join("+"))),
            ComputerAction::Wait { millis } => format!("wait {millis}"),
            // The desktop "launch app" maps to navigating the browser to a URL.
            ComputerAction::OpenApp { name } => format!("navigate {}", shell_quote(name)),
            ComputerAction::AxPress { .. } | ComputerAction::AxSetValue { .. } => {
                return Err(ComputerUseError::InvalidAction(
                    "accessibility-path actions aren't supported by the browser backend; \
                     click/type at coordinates from the screenshot instead"
                        .to_string(),
                ));
            }
            ComputerAction::Observe { .. } | ComputerAction::Screenshot => {
                return Err(ComputerUseError::InvalidAction(
                    "observe/screenshot are not applied via act".to_string(),
                ));
            }
        };
        let detail = self.run(&args).await?;
        Ok(ActionOutcome::ok(detail.trim().to_string()))
    }
}

/// Build the standard Sweet `computer` tool backed by an in-sandbox browser.
/// Coordinates are absolute pixels (the browser reports its own viewport size).
pub fn browser_tool(runner: Arc<dyn CommandRunner>) -> ToolSpec {
    let provider: SharedProvider = Arc::new(E2bComputerUse::new(runner));
    computer_use_tool(provider, CoordinateSpace::Absolute)
}

fn px(v: f64) -> i64 {
    v.round() as i64
}

fn button_name(b: MouseButton) -> &'static str {
    match b {
        MouseButton::Left => "left",
        MouseButton::Right => "right",
        MouseButton::Middle => "middle",
    }
}

/// Single-quote an argument for `/bin/bash -c` (the bridge runs via bash).
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use sweet_core::sandbox::{CommandOutput, SandboxError};

    struct MockRunner {
        last: Mutex<String>,
        observe_out: String,
    }

    #[async_trait]
    impl CommandRunner for MockRunner {
        async fn run(
            &self,
            command: &str,
            _cwd: Option<&std::path::Path>,
            _env: Option<&std::collections::HashMap<String, String>>,
        ) -> Result<CommandOutput, SandboxError> {
            *self.last.lock().unwrap() = command.to_string();
            let stdout = if command.contains("observe") {
                self.observe_out.clone()
            } else {
                "done".to_string()
            };
            Ok(CommandOutput {
                stdout,
                stderr: String::new(),
                exit_code: 0,
            })
        }
    }

    fn provider(observe_out: &str) -> (E2bComputerUse, Arc<MockRunner>) {
        let runner = Arc::new(MockRunner {
            last: Mutex::new(String::new()),
            observe_out: observe_out.to_string(),
        });
        (E2bComputerUse::new(runner.clone()), runner)
    }

    #[tokio::test]
    async fn observe_parses_browserctl_json() {
        // "aGk=" decodes to "hi" — stand-in PNG bytes.
        let (p, _r) = provider(
            r#"{"title":"Example","url":"https://example.com","width":1280,"height":800,
                "outline":"link: More","screenshot_b64":"aGk="}"#,
        );
        let obs = p.observe(&ObserveOptions::default()).await.unwrap();
        assert_eq!(obs.screen_size.width, 1280.0);
        assert_eq!(obs.active_window_title.as_deref(), Some("Example"));
        let tree = obs.accessibility_tree.unwrap();
        assert_eq!(tree.value.as_deref(), Some("link: More"));
        assert_eq!(obs.screenshot.unwrap().data, b"hi");
    }

    #[tokio::test]
    async fn actions_map_to_browserctl_verbs() {
        let (p, r) = provider("{}");

        p.act(&ComputerAction::Click {
            x: 12.4,
            y: 30.6,
            button: MouseButton::Left,
        })
        .await
        .unwrap();
        assert_eq!(*r.last.lock().unwrap(), "browserctl click 12 31 left");

        p.act(&ComputerAction::OpenApp {
            name: "https://e2b.dev".to_string(),
        })
        .await
        .unwrap();
        assert_eq!(
            *r.last.lock().unwrap(),
            "browserctl navigate 'https://e2b.dev'"
        );

        p.act(&ComputerAction::TypeText {
            text: "it's me".to_string(),
        })
        .await
        .unwrap();
        assert_eq!(*r.last.lock().unwrap(), "browserctl type 'it'\\''s me'");
    }

    #[tokio::test]
    async fn accessibility_actions_are_rejected() {
        let (p, _r) = provider("{}");
        let err = p
            .act(&ComputerAction::AxPress {
                element: "0/1".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ComputerUseError::InvalidAction(_)));
    }
}
