// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable "task-as-run" template for a fan-out → reduce agent pipeline: a
//! parallel review workload (6 parallel checkers → coordinator → writer).
//!
//! The parent run spawns N checker **child runs** (`JoinMode::All`); when they
//! join, it runs the coordinator and writer as **durable steps** (crash-safe,
//! memoized) and records the report as an artifact. A worker crash at any point
//! resumes from the last persisted child/step — no checker re-run, no re-upload.
//! This is the orchestration shape a consumer wires real agents into.

use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

use sweet_agent::{Agent, TurnResult};
use sweet_core::message::{Message, Role};
use sweet_core::model::Model;
use sweet_core::tool::ToolSpec;

use suxel_core::driver::{Advance, AgentDriver, ChildSpec, RunContext};
use suxel_core::{Engine, InMemoryBackend, JoinMode, RetryPolicy, RunSpec, RunStatus};
use suxel_sweet::{AgentFactory, SweetAgentDriver};

const N_CHECKERS: usize = 6;

fn last_user_text(messages: &[Message]) -> String {
    messages
        .iter()
        .rev()
        .find(|m| m.role == Role::User)
        .map(|m| m.text_content())
        .unwrap_or_default()
}

/// A checker model: echoes a verdict derived from its task.
struct EchoModel;
#[async_trait]
impl Model for EchoModel {
    async fn complete(&self, messages: &[Message], _t: &[ToolSpec]) -> sweet_core::Result<Message> {
        Ok(Message::assistant(format!(
            "verdict({})",
            last_user_text(messages)
        )))
    }
}

/// A model that prefixes its input — stands in for the coordinator and writer.
struct PrefixModel(&'static str);
#[async_trait]
impl Model for PrefixModel {
    async fn complete(&self, messages: &[Message], _t: &[ToolSpec]) -> sweet_core::Result<Message> {
        Ok(Message::assistant(format!(
            "{}: {}",
            self.0,
            last_user_text(messages)
        )))
    }
}

/// Run one Sweet agent turn (mock model) inside a durable step, returning its text.
async fn run_one_turn(model: Box<dyn Model>, input: String) -> Result<String, String> {
    let mut agent = Agent::new(model);
    match agent.step(input).await {
        Ok(TurnResult::Message(m)) => Ok(m.text_content()),
        Ok(TurnResult::Handoff { .. }) => Err("unexpected handoff".to_string()),
        Err(e) => Err(e.to_string()),
    }
}

/// The parent orchestrator: fan out checkers, then coordinate + write.
struct ReviewDriver;
#[async_trait]
impl AgentDriver for ReviewDriver {
    async fn advance(&self, ctx: &mut RunContext) -> suxel_core::Result<Advance> {
        let children = ctx.children().await?;

        // Phase 1: no children yet → fan out the checkers and join on all.
        if children.is_empty() {
            let specs = (0..N_CHECKERS)
                .map(|i| ChildSpec {
                    agent_type: "checker".into(),
                    goal: format!("checker-{i}"),
                    ..Default::default()
                })
                .collect();
            return Ok(Advance::SpawnChildren {
                specs,
                join: JoinMode::All,
            });
        }

        // Phase 2: checkers joined → gather verdicts, coordinate, write, complete.
        let summary = children
            .iter()
            .filter_map(|c| {
                c.output
                    .as_ref()
                    .and_then(|o| o.get("text"))
                    .and_then(|t| t.as_str())
                    .map(String::from)
            })
            .collect::<Vec<_>>()
            .join("; ");

        let summary_for_coord = summary.clone();
        let synthesis: String = ctx
            .durable_step("coordinate", RetryPolicy::none(), move || {
                let input = summary_for_coord.clone();
                async move { run_one_turn(Box::new(PrefixModel("synthesis")), input).await }
            })
            .await?;

        let synthesis_for_write = synthesis.clone();
        let report: String = ctx
            .durable_step("write", RetryPolicy::none(), move || {
                let input = synthesis_for_write.clone();
                async move { run_one_turn(Box::new(PrefixModel("report")), input).await }
            })
            .await?;

        ctx.put_artifact(
            "review_report",
            "mem://report.md",
            json!({ "checkers": children.len() }),
        )
        .await?;

        Ok(Advance::Complete {
            output: json!({ "report": report, "checkers": children.len() }),
        })
    }
}

#[tokio::test]
async fn durable_review_pipeline_fans_out_and_reduces() {
    let checker_factory: AgentFactory =
        Arc::new(|_build| Ok(Agent::new(Box::new(EchoModel) as Box<dyn Model>)));

    let engine = Engine::new(Arc::new(InMemoryBackend::new()))
        .with_driver("review", Arc::new(ReviewDriver))
        .with_driver("checker", Arc::new(SweetAgentDriver::new(checker_factory)));

    let id = engine
        .create_run(RunSpec {
            agent_type: "review".into(),
            goal: "review the paper".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    engine.run_until_idle("w").await.unwrap();

    // Parent completed with a report synthesized from all checkers.
    let run = engine.get_run(id).await.unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    let out = run.output.unwrap();
    assert_eq!(out["checkers"], N_CHECKERS);
    let report = out["report"].as_str().unwrap();
    assert!(
        report.starts_with("report: synthesis: verdict(checker-"),
        "report was: {report}"
    );

    // All 6 checker child runs completed.
    let kids = engine.backend().list_children(id).await.unwrap();
    assert_eq!(kids.len(), N_CHECKERS);
    assert!(kids.iter().all(|k| k.status == RunStatus::Completed));

    // The report was recorded as an artifact.
    let artifacts = engine.backend().list_artifacts(id).await.unwrap();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].kind, "review_report");
}
