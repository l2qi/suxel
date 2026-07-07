// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Runs a real Sweet `Agent` (with a mock model) durably through the Suxel engine.

use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use sweet_agent::test_util::MockModel;
use sweet_agent::Agent;
use sweet_core::model::Model;
use sweet_core::tool::{ToolError, ToolHandler, ToolSpec};

use suxel_core::store::Backend;
use suxel_core::{Engine, EventKind, InMemoryBackend, RunId, RunSpec, RunStatus};
use suxel_sweet::{wrap_tool, AgentFactory, SweetAgentDriver};

#[tokio::test]
async fn runs_a_sweet_agent_to_completion_durably() {
    let backend = Arc::new(InMemoryBackend::new());
    let factory: AgentFactory = Arc::new(|_build| {
        let model: Box<dyn Model> = Box::new(MockModel::with_replies(["hello from sweet"]));
        Ok(Agent::new(model))
    });
    let engine =
        Engine::new(backend).with_driver("sweet", Arc::new(SweetAgentDriver::new(factory)));

    let id = engine
        .create_run(RunSpec {
            agent_type: "sweet".into(),
            goal: "hi".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle("w").await.unwrap();

    let run = engine.get_run(id).await.unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    assert_eq!(run.output.unwrap()["text"], "hello from sweet");

    // The run's orchestration was journaled.
    let kinds: Vec<_> = engine
        .backend()
        .read(id, 0)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.kind)
        .collect();
    assert!(kinds.contains(&EventKind::RunCreated));
    assert!(kinds.contains(&EventKind::RunCompleted));
}

struct CountingHandler {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ToolHandler for CountingHandler {
    async fn call(&self, _args: serde_json::Value) -> Result<String, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok("side-effect-done".to_string())
    }
}

#[tokio::test]
async fn wrap_tool_memoizes_side_effects() {
    let calls = Arc::new(AtomicUsize::new(0));
    let spec = ToolSpec::new(
        "count",
        "counts invocations",
        serde_json::json!({ "type": "object" }),
        CountingHandler {
            calls: calls.clone(),
        },
    );
    let backend: Arc<dyn Backend> = Arc::new(InMemoryBackend::new());
    let run_id = RunId::new();
    let wrapped = wrap_tool(spec, run_id, backend);

    // Same args twice: the side effect runs once; the second call is memoized.
    let a = wrapped
        .call_rich(serde_json::json!({ "x": 1 }))
        .await
        .unwrap();
    let b = wrapped
        .call_rich(serde_json::json!({ "x": 1 }))
        .await
        .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "memoized result re-ran the side effect"
    );
    assert_eq!(a.text_content(), "side-effect-done");
    assert_eq!(b.text_content(), "side-effect-done");

    // Different args is a different step key, so it runs again.
    wrapped
        .call_rich(serde_json::json!({ "x": 2 }))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
