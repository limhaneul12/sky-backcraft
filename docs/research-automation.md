# Durable research automation

Sky Backcraft keeps SQLite and the native domain/port/auth settings. Three tagged
MCP tools add research workflow control: `research_suite`, `collection_schedule`
and `storage_maintenance`. Their CLI names use hyphens. All fills remain simulated;
these functions neither submit exchange orders nor establish profitability.

## Suites

Create a `ResearchSuiteRequest` with an `ExperimentSpec` template, a tagged
`design` (`batch` or `walk_forward`) and `cost_sweep.fee_bps` /
`cost_sweep.slippage_bps`. Candidate parameter tuples are exact immutable policy
revisions in `template.policy_selections`; create their definitions with the
existing `policy_write` tool. The template requires schema `3.0`,
`causal_execution: "DECLARED_POLICY_WARMUP"`, and `pit_policy: "STRICT_PIT"`.
Old v1/v2 plans and exports retain their prior execution semantics.

The sweep generates the fee × slippage scenarios, applying the fee to both sides
and maker fees. Spread, impact, latency, market-rule provenance and capital remain
explicit frozen template assumptions. Batch runs every candidate across every
scenario and market. A walk-forward design also supplies `selection_bars`,
`evaluation_bars`, `step_bars` and `embargo_bars`. Evaluation windows do not overlap;
any incomplete trailing range is disclosed rather than evaluated silently.

Each walk-forward fold runs all candidates on its selection range. Rankable
candidates need complete finite results for all predeclared markets/scenarios.
The highest exact aggregate net PnL wins (equal starting capital makes this
ordering equivalent to mean net return); ties use lower drawdown then lexical
policy/revision IDs. These are independent asset accounts, not a shared-capital
portfolio. Unavailable candidates retain reasons. A no-winner fold is explicitly
blocked. The winner and selection digest commit before its held-out evaluation
jobs are admitted. New causal execution uses only declared warmup and the
relevant decision/execution windows, not future observations.

Actions: `create`, `get`, `list`, `cases`, `comparisons`, `pause`, `resume`.
`comparisons` pages carry case/fold/phase, scenario costs, policy, market, exact
financials and semantic/causal lineage. Missing financial/statistical results
remain null with a reason. Pause cancels incomplete managed work; Resume retains
completed cases and retries only explicit incomplete retry intents. Use the owner
tool for cancel/retry: ordinary `job_control` cannot mutate managed children.

Individual limits are 7 candidates, 3 markets, 4 scenarios and 12 folds. Joint
limits also apply: 64 runs, 256 comparison cells and the existing 100,000-event
suite budget. All products use checked arithmetic and fail before admission.
For K candidates, M markets, S scenarios, F folds:

- Batch: S runs and S×K×M cells.
- Walk-forward: 2×F×S runs and F×S×M×(K+1) cells.

CLI `research-suite --request request.json` persists only. Add `--wait` to Create
or Resume to explicitly start execution and wait up to the bounded CLI deadline.
Get/List/Cases/Comparisons never start execution implicitly.

## Collection schedules

`collection_schedule` accepts create/get/list/pause/resume/freshness actions.
A create request specifies markets, interval, lookback bars, cadence seconds and
retry policy. Cadence is 60–86,400 seconds; retries are 1–5 with backoff
60–3,600 seconds. Existing collection row/call/raw bounds still apply.

Schedules run while the server/app runs. A missed period coalesces to the newest
completed UTC candle boundary. Each schedule has one in-flight fire; its stable
request identity and pinned attempt prevent duplicate work after restart or a
lost reply. Ordinary lifecycle interruption is recoverable; transient network
and rate-limit failures use capped backoff. Temporary bans, corrupt input and
permanent failures block until an explicit owner Resume. Forming candles and
missing prices are never synthesized.

Freshness shows expected/latest completed boundaries, age, actual grid gaps and
explicit missing reasons per market. Pause stops new admission and durably
cancels queued/running owned work. Offline CLI schedule management persists or
reads state only; execution belongs to a running server.

## Storage management

`storage_maintenance` provides usage, create_backup, list_backups and
retention_candidates. Usage separates SQLite live/reusable/WAL bytes, raw data,
sealed ledgers, exports and managed backups. Existing research-root/DB quotas
remain unchanged.

A managed backup uses only an opaque request ID and a server-owned sibling
namespace; clients cannot choose filesystem paths. It reuses SQLite online backup
and independent content verification. A published directory with a lost receipt
is verified and reconciled, never overwritten. Read-only verification uses an
immutable SQLite snapshot connection so it cannot add WAL/shared-memory files.
Backup limits are 8 snapshots and 4 GiB aggregate; reconciliation enforces the
same caps as first publication.

Retention uses cutoff, keep_recent, offset and page limit. It only proposes
registered resources and embeds the existing deletion preview. Shared data,
active jobs and retained suite/schedule lineage remain protected. Actual removal
uses only `resource_delete_preview` / `resource_hard_delete` and their scoped
expiring confirmation. Suite/schedule parent deletion releases links and retains
ordinary runs/datasets by default. Backups use the same safe deletion journal.
Listing candidates never deletes or starts computation.

## Ownership and verification

`JobRuntime` owns a lightweight producer coordinator and the existing sole
execution worker. Child job creation and case/fire association commit together;
queue saturation defers without advancing producer state. Failures are durable
owner states, not unbounded automatic retries. Both tasks are joined before
storage closes. The SQLite owner remains one bounded OS-thread actor.

`cargo xtask ci` is the canonical settled-source gate. Runtime acceptance also
requires actual SQLite/MCP suites and schedule restart, backup restore and
retention safety journeys. Public endpoint and ChatGPT invocation receipts are
separate and source-fingerprint scoped. A focused test pass alone is not the
whole-feature acceptance record.
