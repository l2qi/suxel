// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Runs the shared engine conformance suite against the SQLite backend.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use suxel_core::store::Backend;
use suxel_core::testkit;
use suxel_store_sqlite::SqliteStore;

#[tokio::test]
async fn sqlite_passes_engine_conformance() {
    let dir = tempfile::tempdir().unwrap();
    let counter = AtomicU32::new(0);
    let make = || {
        let n = counter.fetch_add(1, Ordering::SeqCst);
        let path = dir.path().join(format!("conformance-{n}.db"));
        Arc::new(SqliteStore::open(&path).unwrap()) as Arc<dyn Backend>
    };
    testkit::run_all(make).await;
}

#[tokio::test]
async fn reopening_a_db_recovers_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("persist.db");

    // Park a run waiting on a signal, then drop the store (simulating shutdown).
    let id = {
        use suxel_core::{Engine, RunSpec, RunStatus};
        let backend = Arc::new(SqliteStore::open(&path).unwrap());
        let engine = Engine::new(backend).with_driver("sig", Arc::new(testkit::SignalDriver));
        let id = engine
            .create_run(RunSpec {
                agent_type: "sig".into(),
                goal: "wait".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        engine.run_until_idle("w").await.unwrap();
        assert_eq!(
            engine.get_run(id).await.unwrap().status,
            RunStatus::WaitingSignal
        );
        id
    };

    // Reopen from disk and resume to completion.
    {
        use suxel_core::{Engine, RunStatus};
        let backend = Arc::new(SqliteStore::open(&path).unwrap());
        let engine = Engine::new(backend).with_driver("sig", Arc::new(testkit::SignalDriver));
        engine
            .signal(id, "go", serde_json::json!({ "ok": true }))
            .await
            .unwrap();
        engine.run_until_idle("w").await.unwrap();
        assert_eq!(
            engine.get_run(id).await.unwrap().status,
            RunStatus::Completed
        );
    }
}
