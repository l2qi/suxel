# Agent Guide — suxel

Operational brief for coding agents (Claude Code, etc.) working in this repo. Read this before touching anything.

This file is `AGENTS.md`; `CLAUDE.md` is a symlink to it, so Claude Code and AGENTS.md-reading agents see the same brief. Always edit `AGENTS.md` — never replace the symlink with a separate file.

## What lives here

Suxel is a **Rust-native durable execution runtime for tool-using agents** — Temporal-inspired, but
**agent-run-first**, not workflow-first. It is the durability and orchestration layer that the
[Sweet](https://github.com/sweet/sweet) agent framework deliberately lacks. Sweet stays the agent
framework; Suxel makes a Sweet agent's work a first-class,
addressable, **resumable Run** that survives process and host crashes.

What Suxel owns (and Sweet does not):

- A first-class, addressable, **resumable Run** with a state machine.
- An **event-sourced orchestration log** — distinct from the chat transcript Sweet persists in a `Session`.
- **Durable steps**: idempotent, crash-safe, "never repeat a completed side effect."
- A **worker + queue** model decoupling execution from any HTTP request or process lifetime.
- **Durable signals / approvals / timers** — park for *days*, not a synchronous callback.
- **Durable child runs** with join semantics (Sweet's subagents are in-process `task_local!`s that die with the process).
- **Resource leases, budgets, artifacts**, and run-level observability.

### Crates

| Crate | Depends on | Responsibility |
|---|---|---|
| `suxel-core` | **no Sweet dep**; serde, tokio, async-trait, chrono, uuid | Domain types (`Run`, `Event`, `Step`, `Signal`, `Resource`, `Lease`, `Artifact`, `Budget`), storage **traits**, the `Engine` + worker loop, the durable-step executor, retry/timer/signal/approval/child-run machinery, the `AgentDriver` seam, `InMemoryBackend`, the durable sandbox lifecycle (`SandboxProvider` + `provision_sandbox`/`reap_sandboxes`), and a `testkit` feature (mock drivers + backend conformance scenarios). |
| `suxel-store-sqlite` | `suxel-core` | `SqliteStore` — the full `Backend` over rusqlite + r2d2. Dev/embedded + local single-node. |
| `suxel-store-postgres` | `suxel-core` | `PostgresStore` — the full `Backend` over sqlx (`SELECT … FOR UPDATE SKIP LOCKED` queue). Multi-tenant scale / production. |
| `suxel-sweet` | `suxel-core`, `sweet-core`, `sweet-agent` | **The adapter — the only crate that knows Sweet.** `SweetAgentDriver` (rehydrate a Sweet `Agent`, run one turn, map the outcome to an `Advance`), `DurableIo` (impl `sweet_agent::AgentIo`, journals events + routes approvals to durable signals), `wrap_tool` (memoizing `DurableToolHandler`). |
| `suxel-api` | `suxel-core`, axum, tower | HTTP surface: a `Router` **factory** over `Arc<Engine>` — run CRUD, signal/approve/cancel, and a resumable SSE event stream. The host owns auth + tenancy and mounts this under its own middleware. |
| `suxel-sandbox-e2b` | `suxel-core`, `sweet-core`, `sweet-computer-use-core` | E2B cloud-sandbox provider. `E2bProvider` implements the `SandboxProvider` lifecycle (create / keepalive / kill); `E2bSandbox` (the `bridge` module) implements Sweet's `CommandRunner` + `Filesystem` over the in-sandbox `envd` service, so unchanged Sweet tools run inside a leased sandbox; `E2bComputerUse` (the `computer` module) implements Sweet's `ComputerUseProvider` by driving an in-sandbox `browserctl` CLI. First of a provider family. |

Dependency direction (one-way — never reverse):

```
suxel-store-sqlite    → suxel-core
suxel-store-postgres  → suxel-core
suxel-api             → suxel-core
suxel-sandbox-e2b     → suxel-core, sweet-core, sweet-computer-use-core
suxel-sweet           → suxel-core, sweet-core, sweet-agent
```

**The architectural invariant:** `suxel-core` has **zero** `sweet-*` dependencies. The runtime can in
principle orchestrate non-Sweet workloads; Sweet is one driver behind the `AgentDriver` trait. Verify
with `cargo tree -p suxel-core` — it must show no `sweet-*` crate. Do not add a Sweet dep to core to
make something convenient; put it in `suxel-sweet`.

## The Sweet dependency

Suxel builds on the [Sweet](https://github.com/sweet/sweet) agent framework. `suxel-sweet` and
`suxel-sandbox-e2b` pin Sweet as a **git dependency at a release tag**
(`git = "https://github.com/sweet/sweet", tag = "v0.3.7"`), and the committed `Cargo.lock` records the
exact `git+…` commit — so a plain `cargo build`, and CI, resolve Sweet straight from the pinned tag with
no extra setup. To move to a newer Sweet, bump the `tag =` pins in the `suxel-sweet` and
`suxel-sandbox-e2b` manifests (their `Cargo.lock` entries re-resolve on the next build).

### Developing against a local Sweet checkout

To hack on Sweet and Suxel together, clone Sweet next to this repo (`../sweet`) and uncomment the
`[patch."https://github.com/sweet/sweet"]` block in the root `Cargo.toml` — it redirects the Sweet crates
to `../sweet/crates/*`, so builds use your working tree instead of the pinned tag.

This triggers the **lockfile gotcha**: building with the patch active rewrites `Cargo.lock`, replacing
the pinned `git+…` sources with path sources. **Never commit that.** Re-comment the `[patch]` block (or
`git checkout Cargo.lock` if you touched no dependencies) before committing, so the committed lockfile
keeps its pinned `git+…` sources.

## Backward compatibility

Suxel is pre-1.0 (`0.x`) and has exactly one consumer today (via `suxel-sweet`/`suxel-api`).
Breaking changes are allowed when they make the API simpler or more correct — but they must be
deliberate: update every call site in this workspace and in the consumer, and call the break out in the
commit message. No deprecation shims or compatibility re-exports for names removed in the same change —
remove them cleanly. Intentional, documented breaks; never silent ones or churn for its own sake.

## Quality bar

In order:

1. **Correctness** — tests pass, including the full `--workspace --all-features` suite. The durability
   guarantee is the product: once a `durable_step`'s result is **journaled**, its side effect is never
   re-run — a completed step is served from the journal, even across a crash. The one unavoidable window
   is a crash *after* `f` succeeds but *before* its result is journaled: replay re-invokes `f` (so it is
   at-least-once for un-journaled effects, like every durable runtime). Write `f` to be idempotent /
   retry-safe when its side effect is not itself transactional.
2. **Simplicity (KISS)** — the simplest solution that works. No defensive complexity, no speculative
   abstractions.
3. **DRY** — no copy-paste logic. Shared logic lives in `suxel-core`; stores implement traits, they
   don't reinvent orchestration.
4. **Cohesion** — every module/struct/fn does one thing.
5. **Test coverage** — new behavior needs tests; new error paths need tests. New storage backends must
   pass the shared `testkit` conformance scenarios.

Anti-patterns to avoid:
- Half-finished implementations (no TODO stubs committed).
- Abstractions for hypothetical future requirements.
- Comments that describe what the code does rather than why a non-obvious choice was made.
- `unwrap()` in production code paths. Use `?` or a typed error.

## Security — hard rules

- **Never hardcode API keys, tokens, passwords, or connection strings.** Read them from the environment
  at startup (or via `from_env()`) and fail fast with a clear typed error if missing — see
  `E2bError::MissingApiKey` and `suxel-sandbox-e2b`'s `DEFAULT_API_KEY_ENV` for the established pattern.
- Secrets (e.g. `E2B_API_KEY`, the Postgres URL) live in the host's environment / `.env` (gitignored),
  never in a tracked file, a `config.toml`, or source.

## Established code patterns

### Error handling

- Library crates use `thiserror`, define typed error enums, and expose
  `pub type Result<T> = std::result::Result<T, Error>`.
- Cross-crate conversion: implement `From<…>` and rely on `?`. No string-typed errors in library code.
  (Note: `SandboxProvider`'s trait methods return `Result<_, String>` deliberately — a provider error is
  opaque to the engine and gets journaled as a step failure; that's the one boundary where a string is
  the right type.)

### Async

- Runtime is `tokio`. Async traits use `#[async_trait]` for dyn-compatibility (the `Backend` trait
  bundle, `AgentDriver`, `SandboxProvider` all do).
- Libraries stay runtime-agnostic where they can; the `Engine` worker loop owns its own task structure.

### Storage backends

- A backend implements the trait bundle (`RunStore` / `EventLog` / `StepStore` / `Queue` /
  `SignalStore` / `ResourceStore` / `ArtifactStore` / `Clock`), composed into the blanket-impl'd
  `Backend` supertrait. `Engine` holds `Arc<dyn Backend>`.
- Both stores implement the **same logical schema**: `runs`, `events` (append-only, `(run_id, seq)`),
  `durable_steps` (`(run_id, step_key)` unique → idempotency), `resources`/`leases`, `signals`,
  `timers`, `artifacts`, `queue`. **Never duplicate the chat transcript** — that stays in Sweet's
  `SqliteSession`, referenced by `runs.session_ref`.
- The queue claim is atomic and lease-based: SQLite uses `BEGIN IMMEDIATE`; Postgres uses
  `FOR UPDATE SKIP LOCKED`. A crashed worker's lease expires and another worker re-claims.
- **Any new backend must pass the `testkit` conformance scenarios** (`suxel-core` `testkit` feature) —
  that's how we keep SQLite and Postgres behaviorally identical.

### Sandbox providers

- A remote sandbox is a Suxel `Resource { kind: CodeSandbox, … }`. A provider implements
  `SandboxProvider` (`create` / `keepalive` / `destroy`); the durable lifecycle
  (`provision_sandbox` → memoized create + leased resource → re-attach by persisted id;
  `reap_sandboxes` → destroy + release) lives in `suxel-core::sandbox` and is provider-agnostic.
- Each provider is its own crate (`suxel-sandbox-<name>`), follows the
  `DEFAULT_BASE_URL` / `DEFAULT_API_KEY_ENV` / `new` / `from_env` / `with_*` builder convention, and
  ships **hermetic wiremock tests** for its wire format. No live network calls in the default test run;
  any test that needs a real key/sandbox is `#[ignore]`d.

### Tests

- Unit tests: `#[cfg(test)] mod tests { … }` inside the source file.
- Integration tests: `crates/<crate>/tests/*.rs`. Async tests: `#[tokio::test]`.
- Backend tests run the shared `testkit` scenarios against each store.
- Postgres integration tests are gated on `$SUXEL_TEST_POSTGRES_URL` (skipped when unset).
- Provider tests use `wiremock`; live-service tests are `#[ignore]`d and keyed off an env var.

## Mandatory pre-commit checklist

Run `./scripts/check.sh`, which mirrors CI:

```bash
export RUSTFLAGS=-Dwarnings RUSTDOCFLAGS=-Dwarnings
cargo fmt --all
cargo clippy --workspace --all-targets --all-features
cargo check --workspace
cargo test --workspace --all-features
cargo doc --workspace --no-deps --all-features
```

A hermetic run needs no database or network (Postgres tests skip without `$SUXEL_TEST_POSTGRES_URL`;
the E2B live test is `#[ignore]`d). If you change `check.sh`, change CI too — drift is how "passes
locally, fails CI" happens.

## Git hygiene

- **Never run `git commit` or `git push` (or open a PR) without the owner's explicit approval.** A green
  checklist is a prerequisite, not authorization.
- Do not amend or rewrite an existing commit on your own initiative. Default to a follow-up commit; if
  amending seems better, ask first.
- Fork flow: `origin` = your fork (push here), `upstream` = `suxel/suxel` (PR target). The
  `/pr` command handles this.
- Before committing, ensure no patched `Cargo.lock` (re-comment the root `[patch]` block, or
  `git checkout Cargo.lock`) per the lockfile gotcha above.
- Commit subjects: imperative mood, ≤72 chars.

## Crate-by-crate quick reference

### suxel-core

The generic runtime, no Sweet knowledge. Domain types in `run.rs` / `event.rs` / `step.rs` /
`signal.rs` / `resource.rs` / `artifact.rs` / `budget.rs` / `ids.rs` (UUIDv7 newtypes); storage traits
in `store.rs`; `InMemoryBackend` in `mem.rs`; the `AgentDriver` seam + `RunContext` (with
`durable_step`, `emit`, `take_signal`, `take_approvals`, `children`, `lease_resource`, `put_artifact`,
budget charges) in `driver.rs`; the `Engine` + worker loop + child-join in `engine.rs`; the durable
sandbox lifecycle in `sandbox.rs`. Feature `testkit` ships reusable mock drivers + backend conformance
scenarios — enable it under `[dev-dependencies]` in a store crate to run the shared suite.

The `Advance` enum is the driver→engine contract: `Continue`, `WaitForApproval`, `WaitForSignal`,
`Sleep`, `SpawnChildren { specs, join }`, `Complete`, `Fail`. `JoinMode` is `All` / `Any` / `Quorum(n)`.

### suxel-store-sqlite

`SqliteStore` — full `Backend` over rusqlite + r2d2, JSON text columns, epoch-millis `i64` timestamps,
`BEGIN IMMEDIATE` claims. Construct over a path or in-memory; runs the `testkit` conformance suite.

### suxel-store-postgres

`PostgresStore` — full `Backend` over sqlx. `connect` / `from_pool` / `reset`. `FOR UPDATE SKIP LOCKED`
queue. Integration tests gated on `$SUXEL_TEST_POSTGRES_URL`.

### suxel-sweet

The Sweet adapter. `SweetAgentDriver` rehydrates a Sweet `Agent` (via an `AgentFactory` /
`BuildContext` the host supplies) and runs one turn, choosing `resume_with_approvals` vs
`step_stream_interruptible` based on `agent.has_pending_approvals()`. `DurableIo` implements
`sweet_agent::AgentIo`: journals tool calls/progress to the event log and returns
`ApprovalDecision::Defer` for any call lacking a recorded decision (parking the run). `wrap_tool`
memoizes a tool's side effect through the `StepStore`. This is the only crate that depends on the Sweet
*agent loop* (`sweet-agent`); `suxel-sandbox-e2b` also touches `sweet-core` (for the sandbox traits),
but `suxel-core` itself stays Sweet-free.

### suxel-api

`router(engine) -> axum::Router` — a factory the host mounts under its own auth. Routes: run CRUD,
`signal` / `approve` / `cancel`, an events cursor endpoint, and `/runs/{id}/stream` (resumable SSE via
`async-stream`, replays from `?from=` then live-tails). Owns no auth and no user model — the host stamps
tenancy onto `RunSpec`.

### suxel-sandbox-e2b

`E2bProvider` implementing `SandboxProvider` over E2B's control plane (`https://api.e2b.app`,
`X-API-Key`): `POST /sandboxes` (create), `POST /sandboxes/{id}/timeout` (keepalive),
`DELETE /sandboxes/{id}` (kill, tolerates 404). Builder: `new` / `from_env` / `with_base_url` /
`with_template` / `with_timeout_secs` / `with_http_client`. `create_sandbox` returns a `CreatedSandbox`
(id + `domain` + `envdAccessToken`) for wiring the bridge.

The `bridge` module implements the agent-facing half: `E2bSandbox` (a Sweet `Sandbox` =
`CommandRunner` + `Filesystem`) talks to the in-sandbox `envd` service — Connect unary RPCs at
`/filesystem.Filesystem/*` for metadata, the HTTP `/files` endpoint for byte transfer, and the Connect
*server-streaming* `/process.Process/Start` for exec (enveloped `start`/`data`/`end` frames). Auth is the
run-as user as Basic-auth username plus an optional `X-Access-Token` for a secured sandbox. Hermetic
wiremock tests pin every wire shape; the lifecycle and bridge each have an `#[ignore]`d live test behind
`E2B_API_KEY` (`live_lifecycle`, `live_bridge`).

The `computer` module adds browser / computer-use: `E2bComputerUse` implements Sweet's
`ComputerUseProvider` by driving an in-sandbox `browserctl` CLI (a thin Playwright/CDP wrapper) through
the bridge's `CommandRunner` — so a Sweet agent gets the standard `computer` tool over a headless
browser in the sandbox. It needs only a `CommandRunner`, so it pairs with any sandbox; the sandbox image
must ship `browserctl` exposing the `observe` / action contract documented on the module. Hermetic tests
drive it through a fake runner (no live computer-use test).

`provision_e2b_sandbox(ctx, provider)` is the durable entry point: it provisions (or, after a crash,
re-attaches) via `provision_sandbox`, then rebuilds `E2bSandbox` from the lease's persisted connection
metadata (`from_resource_metadata`, reusing the provider's HTTP pool). The remaining work is a *consumer*
— a code-execution agent class in the host that leases a sandbox and runs Sweet file/bash tools through it.

## What to update when you change things

| Change | Also update |
|--------|------------|
| New storage backend | New `suxel-store-<name>` crate, root `Cargo.toml` members, run the `testkit` suite, this table + crate reference |
| New sandbox provider | New `suxel-sandbox-<name>` crate, wiremock tests, crate reference |
| New `suxel-api` route | The endpoint table in this file, a test |
| Public API change in `suxel-core` | Every store + the `suxel-sweet` adapter + `suxel-api` |
| New crate | Root `Cargo.toml` members and this file |
