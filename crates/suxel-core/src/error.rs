// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Error and result types for the durable runtime.

use crate::ids::RunId;
use thiserror::Error;

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors raised by the runtime engine and storage backends.
#[derive(Debug, Error)]
pub enum Error {
    /// A run id was not found in the store.
    #[error("run not found: {0}")]
    RunNotFound(RunId),

    /// No driver was registered for a run's `agent_type`.
    #[error("no driver registered for agent_type {0:?}")]
    NoDriver(String),

    /// (De)serialization of a payload or step output failed.
    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),

    /// A storage backend operation failed.
    #[error("storage: {0}")]
    Storage(String),

    /// A durable step exhausted its retry budget without succeeding.
    #[error("step {key:?} exhausted after {attempts} attempts: {message}")]
    StepExhausted {
        /// The step key.
        key: String,
        /// How many attempts were made.
        attempts: u32,
        /// The final attempt's error message.
        message: String,
    },

    /// A run exceeded one of its budget limits.
    #[error("budget exceeded: {0}")]
    BudgetExceeded(String),

    /// A driver returned an error from `advance`.
    #[error("driver: {0}")]
    Driver(String),

    /// A generic message error.
    #[error("{0}")]
    Message(String),
}

impl Error {
    /// Construct a storage error from any displayable value.
    pub fn storage(e: impl std::fmt::Display) -> Self {
        Error::Storage(e.to_string())
    }

    /// Construct a driver error from any displayable value.
    pub fn driver(e: impl std::fmt::Display) -> Self {
        Error::Driver(e.to_string())
    }
}
