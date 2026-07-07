# suxel

A Rust-native **durable execution runtime for tool-using agents** — Temporal-inspired, but
agent-run-first rather than workflow-first.

Suxel turns an agent's work into a first-class, addressable, **resumable Run** that survives process and
host crashes. It is the durability and orchestration layer that an agent framework (e.g.
[Sweet](https://github.com/sweet/sweet)) deliberately leaves out:

- **Resumable Runs** with a state machine and an event-sourced orchestration log.
- **Durable steps** — idempotent and crash-safe: a completed side effect never re-runs.
- A **worker + queue** model that decouples execution from any HTTP request or process lifetime.
- **Durable signals, approvals, and timers** — park for days, not a synchronous callback.
- **Durable child runs** with `All` / `Any` / `Quorum` join semantics.
- **Resource leases, budgets, and artifacts**, plus a resumable event stream for UI + audit.

## Crates

| Crate | What it is |
|---|---|
| `suxel-core` | The generic runtime: domain types, storage traits, the `Engine` + worker, durable steps, signals/timers/approvals, child runs, leases, budgets, and the durable sandbox lifecycle. No agent-framework dependency. |
| `suxel-store-sqlite` | `SqliteStore` backend (rusqlite). Embedded / local. |
| `suxel-store-postgres` | `PostgresStore` backend (sqlx, `FOR UPDATE SKIP LOCKED`). Multi-tenant scale. |
| `suxel-sweet` | Adapter that drives a [Sweet](https://github.com/sweet/sweet) agent as a Suxel run. |
| `suxel-api` | An Axum router factory: run CRUD, signal/approve/cancel, resumable SSE event stream. |
| `suxel-sandbox-e2b` | [E2B](https://e2b.dev) cloud-sandbox provider for the durable sandbox lifecycle. |

`suxel-core` carries no agent-framework dependency; an `AgentDriver` trait is the seam, and `suxel-sweet`
is one implementation of it.

## Status

Pre-1.0 and under active development. The runtime, both storage backends, the Sweet adapter, the HTTP
surface, and the full E2B sandbox integration — lifecycle, durable provision/re-attach, and the
exec/filesystem bridge — are in place (verified hermetically, with `#[ignore]`d live end-to-ends behind
`E2B_API_KEY`). What remains is a consuming code-execution agent in the host application.

## Contributing

See [`AGENTS.md`](./AGENTS.md) for the architecture, the dependency invariants, the code patterns, and
the pre-commit checklist. Run `./scripts/check.sh` before sending a change.

## License

Apache-2.0. See [LICENSE](./LICENSE).
