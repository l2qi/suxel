// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end durable human-in-the-loop: a Sweet agent's dangerous tool call
//! parks the run for approval; after `engine.approve`, the run resumes from the
//! persisted session and finishes — without re-invoking the model for the first
//! turn.

use async_trait::async_trait;
use std::sync::Arc;

use sweet_agent::test_util::MockTool;
use sweet_agent::Agent;
use sweet_core::message::{Message, Role, ToolCall};
use sweet_core::model::Model;
use sweet_core::tool::ToolSpec;
use sweet_session::SqliteSession;

use suxel_core::{
    ApprovalDecision, ApprovalResolution, Engine, InMemoryBackend, RunSpec, RunStatus,
};
use suxel_sweet::{wrap_tool, AgentFactory, SweetAgentDriver};

/// A model that requests one `echo` tool call, then replies "done" once it sees
/// a tool result. Deterministic on the message history, so it can be rebuilt
/// fresh on every durable advance.
struct OneToolThenDone;

#[async_trait]
impl Model for OneToolThenDone {
    async fn complete(
        &self,
        messages: &[Message],
        _tools: &[ToolSpec],
    ) -> sweet_core::Result<Message> {
        if messages.iter().any(|m| m.role == Role::Tool) {
            Ok(Message::assistant("done"))
        } else {
            Ok(Message::with_tool_calls(vec![ToolCall {
                id: "call_1".into(),
                name: "echo".into(),
                arguments: serde_json::json!({ "msg": "hi" }),
            }]))
        }
    }
}

#[tokio::test]
async fn run_parks_for_approval_then_resumes_to_completion() {
    let dir = tempfile::tempdir().unwrap();
    let session_path = dir.path().join("sess.db").to_string_lossy().to_string();

    let backend = Arc::new(InMemoryBackend::new());
    let factory: AgentFactory = Arc::new(|build| {
        let path = build.session_ref.clone().ok_or("missing session_ref")?;
        let session = SqliteSession::open(&path).map_err(|e| e.to_string())?;
        let model: Box<dyn Model> = Box::new(OneToolThenDone);
        let spec: ToolSpec = MockTool::echoing("echo").into();
        let tool = wrap_tool(spec, build.run_id, build.backend.clone());
        Ok(Agent::new(model).with_session(session).with_tool(tool))
    });
    let engine = Engine::new(backend).with_driver("hitl", Arc::new(SweetAgentDriver::new(factory)));

    let id = engine
        .create_run(RunSpec {
            agent_type: "hitl".into(),
            goal: "do the dangerous thing".into(),
            session_ref: Some(session_path),
            ..Default::default()
        })
        .await
        .unwrap();

    // First pass: the dangerous tool call parks the run for approval.
    engine.run_until_idle("w").await.unwrap();
    assert_eq!(
        engine.get_run(id).await.unwrap().status,
        RunStatus::WaitingApproval
    );

    // Approve, then resume: the tool runs and the turn finishes.
    engine
        .approve(
            id,
            ApprovalResolution {
                tool_call_id: "call_1".into(),
                decision: ApprovalDecision::Allow,
            },
        )
        .await
        .unwrap();
    engine.run_until_idle("w").await.unwrap();

    let run = engine.get_run(id).await.unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    assert_eq!(run.output.unwrap()["text"], "done");
}

#[tokio::test]
async fn denied_approval_completes_with_denial() {
    let dir = tempfile::tempdir().unwrap();
    let session_path = dir.path().join("sess.db").to_string_lossy().to_string();

    let backend = Arc::new(InMemoryBackend::new());
    let factory: AgentFactory = Arc::new(|build| {
        let path = build.session_ref.clone().ok_or("missing session_ref")?;
        let session = SqliteSession::open(&path).map_err(|e| e.to_string())?;
        let model: Box<dyn Model> = Box::new(OneToolThenDone);
        let spec: ToolSpec = MockTool::echoing("echo").into();
        Ok(Agent::new(model).with_session(session).with_tool(spec))
    });
    let engine = Engine::new(backend).with_driver("hitl", Arc::new(SweetAgentDriver::new(factory)));

    let id = engine
        .create_run(RunSpec {
            agent_type: "hitl".into(),
            goal: "do it".into(),
            session_ref: Some(session_path),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle("w").await.unwrap();
    assert_eq!(
        engine.get_run(id).await.unwrap().status,
        RunStatus::WaitingApproval
    );

    // Deny: the tool does not run; the model sees the denial result and still
    // produces a final message, so the run completes (not fails).
    engine
        .approve(
            id,
            ApprovalResolution {
                tool_call_id: "call_1".into(),
                decision: ApprovalDecision::Deny,
            },
        )
        .await
        .unwrap();
    engine.run_until_idle("w").await.unwrap();

    let run = engine.get_run(id).await.unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    assert_eq!(run.output.unwrap()["text"], "done");
}
