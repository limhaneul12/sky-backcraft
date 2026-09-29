# sky-backcraft

`sky-backcraft` is a Rust research system for public Upbit spot-market data,
deterministic strategy simulation, exact accounting, durable research jobs and
portable independent verification. The supported market boundary is
`KRW-BTC`, `KRW-ETH` and `KRW-XRP`, long/cash only.

The system never places an order and never uses an Upbit private API, credential,
account balance or observed fill. Every execution is simulated. Fees, spread,
slippage, impact, tick rules and minimum notional are explicit research inputs,
not claims about a historical market unless the request supplies matching
provenance.

## Current status

The current implementation includes typed contracts, public candle
collection, SQLite and gzip evidence storage, frozen experiment plans, a bounded
durable job runner, pure strategy/evidence/engine logic, fact-ledger reporting,
independent verification/replay, immutable editable policy revisions, bounded
run/job/policy history, and a stateless JSON-first RMCP adapter.

Native `cargo xtask ci` and Linux Docker `make ci` pass with 90 tests.
`make rebuild` deploys the persistent container and retains an existing live Quick
Tunnel address. Public MCP acceptance covers 15 tools, policy create/revise,
immutable history, multiple exact policy revisions, frozen execution, JSON export,
independent verification and replay. The native financial matrix also completed
50 commands including three assets/resolutions, execution sensitivities and
backup/restore. Evidence is scoped by source fingerprint in the
[current verification record](docs/verification-first-use-2026-09-27.md).

`lab_status` conservatively reports `IMPLEMENTED_UNVERIFIED` because the server does
not embed or infer external acceptance receipts. Consult the source-matched delivery
record for executed gates; this runtime label is not a profitability claim or an
independent CI attestation. Missing evidence still blocks S5/control models explicitly.

ChatGPT personal plugin `Sky Backcraft` is registered and its status, policy-list and
run-history tools were invoked successfully in a real Work conversation.

The first-use repairs add complete artifact transfer, all-model comparison summaries,
quantity-weighted cost reporting, linked exit reasons, truthful terminal progress,
structured limit reports and evidence-labelled gap classification. Existing run hashes
and ledger bytes were preserved; see [first-use verification](docs/verification-first-use-2026-09-27.md).

## Architecture

```text
CLI / stateless RMCP
        |
        v
durable job + collection orchestration
        |
        +--> public Upbit client
        +--> immutable policy registry + finite rules evaluator
        +--> pure strategy / evidence / engine
        +--> reporting / independent verifier / replay
        |
        v
bounded DatabaseOwner thread
        |
        +--> SQLite catalogs and facts
        +--> content-addressed gzip raw objects and exports
```

- SQLite has one process owner and a bounded command queue. Startup and final join
  stay outside Tokio worker threads.
- Raw HTTP bytes are published before database references. Dataset, plan, job,
  attempt, run and artifact identities remain typed and immutable.
- Economic arithmetic uses checked decimal values. Floating point is confined to
  statistical indicators and metrics and must remain finite. Serde JSON keeps both
  `arbitrary_precision` and `float_roundtrip` so decimal literals and finite metric
  floats retain their intended wire behavior.
- Missing data, insufficient warmup, missing point-in-time evidence and unverified
  market rules are visible states; they are never converted to zero performance.
- Independent verification reconstructs links, cash, quantity, basis, fees, equity
  and drawdown from exported facts without calling engine signal/fill logic.
- Process startup installs one explicit Rustls Ring crypto provider before either
  Reqwest client is built. Direct Upbit HTTPS and RMCP SDK HTTPS share that provider.

See [docs/data-dictionary.md](docs/data-dictionary.md) for the public terms and
[docs/operator-guide.md](docs/operator-guide.md) for command and lifecycle details.
Docker/Cloudflare operation is documented separately in
[docs/docker.md](docs/docker.md).

## Quick start

Use the pinned Rust toolchain. This machine keeps build artifacts on the configured
SSD; CI may use another build directory.

```sh
export CARGO_TARGET_DIR=/Volumes/256.SSD/RustBuild/sky-backcraft

cargo run -p spot-lab -- probe \
  --market KRW-BTC --interval h1 --count 2 --data-root data

cargo run -p spot-lab -- collect \
  --request requests/collect.json --data-root data

cargo run -p spot-lab -- plan \
  --request requests/plan.json --data-root data

cargo run -p spot-lab -- job \
  --request requests/job.json --data-root data

cargo run -p spot-lab -- mcp-serve \
  --bind 127.0.0.1 --port 8130 --data-root data

cargo run -p spot-lab -- mcp-selfcheck \
  --url http://127.0.0.1:8130/mcp

cargo run -p spot-lab -- policy-write \
  --request docs/examples/policy-create-custom.json --data-root data

cargo run -p spot-lab -- policy-query \
  --request docs/examples/policy-list.json --data-root data
```

For direct CLI probes, `probe --no-save` deliberately skips raw archival. Long
local `job` submissions and retries retain the process until their current attempt
reaches a terminal state. MCP submissions return durable IDs and are polled with
the typed job-control tool.

## CLI surface

| Command | Input | Behavior |
| --- | --- | --- |
| `probe` | market, interval, completed count | One bounded public Upbit probe; saves raw evidence unless `--no-save` |
| `collect` | `CollectRequest` JSON/TOML | Collects and freezes a dataset, with checkpoints and quality findings |
| `derive-dataset` | source dataset ID and `h1`, `h4` or `d1` | Persists a provenance-linked deterministic resampling |
| `plan` | `PlanRequest` JSON/TOML | Validates explicit assumptions and freezes input/config digests |
| `job` | `JobSubmission` JSON/TOML | Runs a durable collect/backtest/export/verify attempt and waits for terminal state |
| `job-control` | `JobControl` JSON/TOML | Gets, cancels or retries a durable job; retry waits for its new terminal attempt |
| `evidence-register` | `EvidenceImport` JSON/TOML | Validates and stores explicitly acknowledged public, non-sensitive Evidence |
| `policy-write` | tagged `PolicyWrite` JSON/TOML | Creates an immutable custom policy or appends a CAS-checked revision |
| `policy-query` | tagged `PolicyQuery` JSON/TOML | Lists policy heads, reads an exact revision, or pages revision/run history |
| `history-query` | tagged `HistoryQuery` JSON/TOML | Pages immutable run or durable job history |
| `verify-export` | local export directory | Reads the bounded package and performs independent fact verification |
| `replay-export` | local export directory | Replays a qualified package and compares reconstructed results |
| `backup` | new destination directory | Creates an online SQLite/raw/artifact backup with a manifest |
| `restore` | backup and explicit new data root | Restores under an exclusive root lock and revalidates stored data and finalized runs |
| `mcp-serve` | bind, port, data root | Starts the bounded stateless JSON RMCP service and durable runner |
| `mcp-selfcheck` | MCP URL plus probe options | Checks initialize, tool discovery and a bounded public probe |
| `schemas` | output directory | Regenerates public DTO/MCP schemas; the current generator also emits policy/history contracts |

All request files are bounded to 256 KiB. Unknown, malformed and duplicate CLI
options are rejected. JSON/TOML DTOs reject unknown fields where their typed
contract requires it.

## Research assumptions

- Candle ranges are UTC half-open intervals `[start, end)`. Completed candles use
  `candle_date_time_utc`; KST and last-tick timestamps are not candle-time authority.
- Supported data intervals are `m1`, `m5`, `h1`, `h4` and `d1`. A collection request
  must explicitly name its markets, range, data resolution, warmup bars and
  `completed_only=true`.
- S1-S5, Buy-and-Hold and the S1 Evidence coverage control are seeded as editable
  built-in policy families. Editing appends an immutable revision; it never mutates
  a prior run. User policies use the bounded finite rules language, never arbitrary code.
- Experiment schema `2.0` selects exact policy revision references and freezes their
  complete definitions into the resolved plan. Family labels are metadata and never
  select mutable policy heads at execution time.
- Accounts are independent per market/strategy model. Execution, cost, terminal,
  Evidence, market-rule and report-clock policies are all request inputs.
- Evidence is supplied by the operator. The system does not fetch or infer Evidence.
  `STRICT_PIT` and `LATEST_VERSION_PROXY` are distinct policies; absent eligible
  Evidence can produce `BLOCKED_EVIDENCE` or the explicit cash/control behavior.
- Exports contain `manifest.json`, `review.json` and `ledger.json.gz`. The manifest
  hashes the artifacts and frozen dependencies; it does not hash itself.

## Public no-auth mode

The normal server bind is loopback research use. `--public-no-auth` is reserved for
an isolated, transient E2E with public, non-sensitive Upbit research data:

```sh
cargo run -p spot-lab -- mcp-serve \
  --public-no-auth \
  --bind 0.0.0.0 \
  --data-root /path/to/new-or-marked-isolated-root \
  --allow-host actual-issued-host.example
```

The root must be new and empty or already carry the tool's public-research marker.
The actual issued hostname must be allowlisted. Host and Origin checks are routing
controls, not authentication. This mode does not authorize persistent deployment,
private data, arbitrary URLs/SQL/paths, general filesystem access or trading.

HTTPS client support uses RMCP's `reqwest-tls-no-provider` feature plus direct Rustls
`0.23.45` with `ring`, `std` and `tls12`. Platform trust is supplied through
`rustls-platform-verifier`; on macOS that reaches Apple trust APIs through upstream
Security Framework wrappers. No AWS-LC, OpenSSL provider or project-authored unsafe/FFI
is introduced.

## Resource limits

The implementation defines these fixed ceilings:

- 16 open connections, 8 in-flight HTTP requests and 20 requests/second;
- 256 KiB request bodies and 512 KiB responses;
- 30-second idle, 120-second absolute connection lifetime, 20-second request
  deadline and 30-second graceful-shutdown deadline;
- 8 queued durable attempts, one compute job and 32 database commands;
- at most 32 immutable attempts per job;
- 30,000 dataset rows, 21 models/run, 20,000 fact events/model and 100,000/run;
- 128 MiB compressed and 512 MiB decoded export ledger; 1 GiB data-root quota.

These values describe the source contract. Saturation, shutdown, RSS and public E2E
claims require executed evidence and remain pending until recorded separately.

## Verification

Canonical repository gates are:

```sh
cargo xtask rules
cargo xtask fmt
cargo xtask check
cargo xtask test
cargo xtask ci
```

Only executed gates are PASS. Public HTTPS SDK selfcheck is current. The 75-test CI,
local HTTP receipt and financial acceptance are earlier-source evidence. Full latest
`cargo xtask ci` is pending. Remaining lanes include matching-source financial rerun,
native SQLite backup/restore closure, durable job restart/cancel/saturation proof,
stateless transport saturation and shutdown proof, structured-log redaction evidence,
the real three-market experiment matrix, export reread/offline verification,
elapsed/RSS/output-size measurement, transient public tunnel evidence and the intended
client invocation. Local HTTP, tunnel reachability and client execution are separate
claims.

Official exchange references: [minute candles](https://docs.upbit.com/kr/reference/list-candles-minutes),
[day candles](https://docs.upbit.com/kr/reference/list-candles-days), and
[rate limits](https://docs.upbit.com/kr/reference/rate-limits).

## Agent skills

Repository-local skills live in `skill_backcraft/`. Point an agent at the relevant
`SKILL.md`, or register its individual folder with the agent's skill loader:

- [backcraft-experiment](skill_backcraft/backcraft-experiment/SKILL.md): collect data, author/revise policies, freeze plans and run backtests.
- [backcraft-results](skill_backcraft/backcraft-results/SKILL.md): compare existing runs, interpret costs/exits, receive complete files and verify results.

Each folder includes `agents/openai.yaml` discovery metadata. The repository files
alone do not imply installation in a global skill directory or a remote plugin.
