#!/usr/bin/env bash
set -euo pipefail

# Mirrors .github/workflows/ci.yml — same flags, same feature selection — so
# a green local check predicts a green CI run. The only intentional
# difference: CI verifies formatting with `--check`, here fmt fixes it.
#
# The Postgres store's integration tests are gated on $SUXEL_TEST_POSTGRES_URL
# (skipped when unset) and the E2B live test is `#[ignore]`d, so this stays
# hermetic — no database or network required for a green run.
export RUSTFLAGS="${RUSTFLAGS:--Dwarnings}"
export RUSTDOCFLAGS="${RUSTDOCFLAGS:--Dwarnings}"

cargo fmt --all
cargo clippy --workspace --all-targets --all-features
cargo check --workspace
cargo test --workspace --all-features
cargo doc --workspace --no-deps --all-features
