// Copyright (C) 2026 Ryuichi Intellectual Property LLC and the Suxel project contributors
// SPDX-License-Identifier: Apache-2.0

//! Per-run budgets and their running usage.
//!
//! A [`Budget`] caps how much a run (and its fan-out) may consume. Children
//! inherit a slice of the parent's budget; the engine checks usage after every
//! `advance` and fails the run if any limit is breached.

use serde::{Deserialize, Serialize};

/// Ceilings for a single run. `None` means "unlimited" for that dimension.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Budget {
    /// Max total LLM tokens.
    pub max_tokens: Option<u64>,
    /// Max total cost in USD.
    pub max_cost_usd: Option<f64>,
    /// Max wall-clock seconds (enforced by the engine via timers, advisory here).
    pub max_wall_clock_secs: Option<u64>,
    /// Max number of tool calls.
    pub max_tool_calls: Option<u64>,
    /// Max number of child runs this run may spawn.
    pub max_children: Option<u64>,
}

/// Cumulative usage charged against a [`Budget`].
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct BudgetUsage {
    /// Tokens consumed so far.
    pub tokens: u64,
    /// Cost consumed so far, USD.
    pub cost_usd: f64,
    /// Tool calls made so far.
    pub tool_calls: u64,
    /// Children spawned so far.
    pub children: u64,
}

impl BudgetUsage {
    /// Fold another usage delta into this one.
    pub fn add(&mut self, delta: &BudgetUsage) {
        self.tokens += delta.tokens;
        self.cost_usd += delta.cost_usd;
        self.tool_calls += delta.tool_calls;
        self.children += delta.children;
    }
}

impl Budget {
    /// Return `Err(reason)` if `usage` breaches any limit.
    pub fn check(&self, usage: &BudgetUsage) -> std::result::Result<(), String> {
        if let Some(max) = self.max_tokens {
            if usage.tokens > max {
                return Err(format!("tokens {} > {}", usage.tokens, max));
            }
        }
        if let Some(max) = self.max_cost_usd {
            if usage.cost_usd > max {
                return Err(format!("cost_usd {:.4} > {:.4}", usage.cost_usd, max));
            }
        }
        if let Some(max) = self.max_tool_calls {
            if usage.tool_calls > max {
                return Err(format!("tool_calls {} > {}", usage.tool_calls, max));
            }
        }
        if let Some(max) = self.max_children {
            if usage.children > max {
                return Err(format!("children {} > {}", usage.children, max));
            }
        }
        Ok(())
    }
}
