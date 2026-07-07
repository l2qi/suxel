// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Artifacts produced by a run (reports, patches, PDFs, data extracts, …).

use crate::ids::{ArtifactId, RunId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A durable, addressable output of a run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    /// Unique id.
    pub id: ArtifactId,
    /// Producing run.
    pub run_id: RunId,
    /// Application-defined type tag (e.g. `"report"`, `"patch"`, `"pdf"`).
    pub kind: String,
    /// Where the bytes live (a URI the host can resolve).
    pub uri: String,
    /// Arbitrary metadata.
    pub metadata: serde_json::Value,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
}
