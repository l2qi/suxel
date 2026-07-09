// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! The engine conformance suite, run against the in-memory backend. The same
//! scenarios run against every storage backend (see `suxel_core::testkit`).
#![cfg(feature = "testkit")]

use std::sync::Arc;
use suxel_core::store::Backend;
use suxel_core::{testkit, InMemoryBackend};

fn backend() -> Arc<dyn Backend> {
    Arc::new(InMemoryBackend::new())
}

#[tokio::test]
async fn completes_a_simple_run() {
    testkit::assert_completes_simple(backend()).await;
}

#[tokio::test]
async fn durable_step_runs_side_effect_exactly_once() {
    testkit::assert_durable_step_once(backend()).await;
}

#[tokio::test]
async fn durable_step_retries_until_success() {
    testkit::assert_retries(backend()).await;
}

#[tokio::test]
async fn parks_for_signal_then_resumes() {
    testkit::assert_signal_park_resume(backend()).await;
}

#[tokio::test]
async fn parks_for_approval_then_resumes() {
    testkit::assert_approval_park_resume(backend()).await;
}

#[tokio::test]
async fn denied_approval_fails_the_run() {
    testkit::assert_denied_approval_fails(backend()).await;
}

#[tokio::test]
async fn spawns_children_and_joins_all() {
    testkit::assert_join_all(backend()).await;
}

#[tokio::test]
async fn any_join_cancels_the_losers() {
    testkit::assert_join_any_cancels(backend()).await;
}

#[tokio::test]
async fn budget_breach_fails_the_run() {
    testkit::assert_budget_breach(backend()).await;
}

#[tokio::test]
async fn leases_resources_and_produces_artifacts() {
    testkit::assert_resource_and_artifact(backend()).await;
}

#[tokio::test]
async fn selective_signal_consume() {
    testkit::assert_signals_selective_consume(backend()).await;
}

#[tokio::test]
async fn concurrent_appends_get_distinct_seqs() {
    testkit::assert_concurrent_appends_distinct_seq(backend()).await;
}
