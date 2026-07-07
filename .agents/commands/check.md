Run the mandatory pre-commit checklist in order:

1. `cargo fmt --all` — fix formatting, don't just check.
2. `cargo clippy --workspace --all-targets --all-features -- -D warnings` — zero warnings.
3. `cargo test --workspace --all-features`
4. `cargo doc --workspace --no-deps --all-features`

The Postgres integration tests skip unless `$SUXEL_TEST_POSTGRES_URL` is set, and
the E2B live test is `#[ignore]`d — so a hermetic run needs no database or network.

Fix any issues found and re-run failed steps until all pass cleanly.
Do NOT commit — just verify the tree is green.
