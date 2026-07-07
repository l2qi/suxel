// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Runs the shared engine conformance suite against Postgres.
//!
//! Requires a reachable database; set `SUXEL_TEST_POSTGRES_URL` to run it. When
//! the variable is unset the test is skipped (so CI without a DB stays green).

use std::sync::Arc;
use suxel_core::store::Backend;
use suxel_core::testkit;
use suxel_store_postgres::PostgresStore;

#[tokio::test]
async fn postgres_passes_engine_conformance() {
    let Ok(url) = std::env::var("SUXEL_TEST_POSTGRES_URL") else {
        eprintln!("SUXEL_TEST_POSTGRES_URL unset; skipping Postgres conformance test");
        return;
    };

    let store = PostgresStore::connect(&url)
        .await
        .expect("connect to Postgres");
    store.reset().await.expect("reset schema");

    // Scenarios are scoped by run id and each drains the queue, so they can share
    // one schema. Hand each a clone of the (pooled) store.
    let make = || Arc::new(store.clone()) as Arc<dyn Backend>;
    testkit::run_all(make).await;
}
