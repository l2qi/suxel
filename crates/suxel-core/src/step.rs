// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable steps: idempotent, crash-safe units of work.
//!
//! A step's result is journaled by key. On replay (worker crash + re-claim, or
//! an in-turn retry), a completed step returns its saved output instead of
//! re-running the side effect — the "never repeat a completed side effect"
//! guarantee.

use crate::ids::RunId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The idempotency key for a durable step, unique within a run.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StepKey(pub String);

impl std::fmt::Display for StepKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for StepKey {
    fn from(s: &str) -> Self {
        StepKey(s.to_string())
    }
}

impl From<String> for StepKey {
    fn from(s: String) -> Self {
        StepKey(s)
    }
}

/// Terminal status of a journaled step.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    /// Succeeded; `output` holds the serialized result.
    Completed,
    /// Failed terminally after exhausting retries.
    Failed,
}

/// A journaled step result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StepRecord {
    /// The owning run.
    pub run_id: RunId,
    /// The step key.
    pub key: StepKey,
    /// Terminal status.
    pub status: StepStatus,
    /// Serialized output (when `Completed`).
    pub output: Option<serde_json::Value>,
    /// Error message (when `Failed`).
    pub error: Option<String>,
    /// How many attempts were made.
    pub attempts: u32,
    /// Last-update timestamp.
    pub updated_at: DateTime<Utc>,
}

/// Retry policy for a durable step: capped attempts with exponential backoff.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum attempts (including the first). `1` means no retry.
    pub max_attempts: u32,
    /// Base delay before the first retry.
    pub base_delay: Duration,
    /// Cap on the backoff delay.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        }
    }
}

impl RetryPolicy {
    /// A policy that never retries.
    pub fn none() -> Self {
        RetryPolicy {
            max_attempts: 1,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }

    /// Backoff delay before the given attempt number (1-based: the delay *after*
    /// attempt `n` fails, before attempt `n + 1`). Exponential, capped.
    pub fn backoff_for(&self, failed_attempt: u32) -> Duration {
        if self.base_delay.is_zero() {
            return Duration::ZERO;
        }
        let shift = failed_attempt.saturating_sub(1).min(32);
        let factor = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
        let delay = self
            .base_delay
            .saturating_mul(factor.min(u32::MAX as u64) as u32);
        delay.min(self.max_delay)
    }
}
