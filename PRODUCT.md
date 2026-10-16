# Sky Backcraft desktop app

<!-- impeccable:product-schema 1 -->

## Platform

Native macOS desktop and menu bar app built with AppKit.

## Users

The primary user is a local quant-research operator who needs to inspect immutable experiment results, understand which exact policy revision produced them, observe an Upbit account without exposing balances, and operate the local MCP service without a terminal.

## Product Purpose

Sky Backcraft is a native research workspace for strategy-first analysis and evidence-backed operational observation. It keeps historical research, persistent paper capability, live account reads, and live-trading authority visibly separate so that a completed replay or connected exchange can never be mistaken for an active trading session.

## Core Product Flows

1. **Compare one exact strategy.** Choose a registered policy by immutable policy ID, optionally filter by market, and review each run/model as an independent graph. Different markets, evaluation windows, capital modes, and cost assumptions are never averaged or joined into one synthetic curve.
2. **Inspect one immutable result.** Open a run/model to see Decimal-backed return, drawdown, PnL, fees, fills, closed episodes, exact identities, F08 integrity state, equity/drawdown charts, zoom, confirmed fill markers, CSV export, and a read-only cost/ledger window.
3. **Find historical evidence.** Search and filter the run catalogue, page through bounded results, inspect the selected run's policy/dataset provenance, export the filtered scope, and open it in the result view.
4. **Understand a policy.** Search and filter the policy catalogue by origin, historical use, and referenced market. Inspect immutable definition JSON, revision history, and related ledger runs, then move directly to that strategy's performance or a selected run.
5. **Operate persistent paper sessions & observe operations safely.** Create and start genuine stateful paper sessions with frozen policy revisions and independent per-market virtual accounts (`SIMULATED_SCENARIO`), pause/resume/stop sessions with `expected_sequence` CAS and unknown-outcome request readback, inspect paged session events and per-market freshness/warmup, distinguish zero active jobs from paused collection schedules, read masked live balances and open/closed orders, and keep live trading visibly OFF.
6. **Preview policy impact.** Select user policies and request server-owned deletion-impact previews. Policies referenced by persistent paper sessions (`paper:<session_id>`) are protected against deletion. The desktop app does not expose a policy hard-delete action.
7. **Operate the local service.** Use the menu bar and Settings window for the dashboard, balance privacy, fail-closed trading controls, MCP domain/port/authentication, logs, start/stop/restart, and URL or OAuth-code copy where available.

## Implemented Capabilities

- Seven-section native sidebar: `성과 분석`, `실행 이력`, `정책 카탈로그`, `지속 모의 · 작업`, `전략 자동화 · MOCK`, `실계좌 및 주문 조회`, `삭제 영향 미리보기`.
- Strategy-first policy grouping with exact identity and duplicate-name disambiguation.
- Independent run/model cards with their own market, period, cost assumption, capital mode, exact KPIs, equity or drawdown graph, and detail action.
- Read-only ledger detail for fills, orders, order events, episodes, and signals, including exact cost diagnostics and paged records.
- Exact-value CSV exports for strategy results, run details, and filtered history; display rounding is not reused as source data.
- Native policy inventory with definition, revision history, historical usage, related runs, and cross-navigation to evidence.
- Durable local history workspace state for exact filters, pinned runs, up to four selected runs, compact/detail mode, selected metric ordering, and the last opened run. Corrupt or unsupported workspace files fail closed and are not overwritten.
- Genuine persistent paper-session runtime (`paper_session_query`, `paper_session_write`) and native `지속 모의 · 작업` workspace: causal completed-candle continuation, SQLite checkpoint persistence (`0015_paper_sessions.sql`), pre-start warmup without pre-start trades, pause/resume/stop lifecycle with `expected_sequence` CAS, durable request receipts and unknown-outcome readback, independent per-market virtual accounts, and prominent `VIRTUAL / SIMULATED` badges.
- Read-only Upbit account and order monitoring with privacy masking, per-facet `ReadFreshness` (`LIVE`, `DELAYED`, `STALE`, `OFFLINE`), composite preflight+read cancellation, and visible-only automatic refresh under a rate budget.
- Fail-closed live controls: Start, Resume, and Pause remain unavailable while trading is configured OFF. Emergency Stop requires a fresh authenticated server snapshot (expiring after 30s), matching acknowledgement, and state readback; its stated effect is new-order admission blocking only.
- Settings and the private MCP `live_order_control` tool share a managed order-sending switch. It starts OFF after every process start, requires explicit enable confirmation and current boot epoch/sequence, and uses durable request readback after uncertain replies. ON does not start/resume a strategy or clear emergency stop. OFF closes new sends, drains admitted sends before acknowledgement, and locally rejects proven unsent reserved requests without canceling provider orders or liquidating assets.
- Existing menu bar MCP lifecycle, configuration, OAuth/no-auth choices, log access, and owned backend/tunnel lifecycle remain part of the app.

- Strategy automation (`automation_session_query` / `automation_session_write`) uses frozen revisions, finalized candles, actual venue-shaped account sizing, account-wide risk reservations, durable order/fill identities and restart reconciliation. Its native workspace currently creates MOCK sessions only; production live sending remains disabled. Pause/stop blocks new admissions while preserving holdings and unresolved orders.

## Product Truth and Safety Invariants

- `run_id`, `model_id`, policy revision, dataset, window, market, capital mode, cost assumption, and ledger state travel together through every result view and export.
- Economic values originate as Decimal strings or Decimal values. Unknown, invalid, or unavailable values are never silently converted to zero.
- F08 PASS means ledger integrity, not investment quality. A completed zero-fill result remains a no-trade result. In persistent paper sessions, independent F08 verification is explicitly reported as `NOT_AVAILABLE (INDEPENDENT_PAPER_VERIFIER_NOT_IMPLEMENTED)` and never fabricated as `PASS` or `0`.
- Historical replay, queued work, paused collection, persistent paper session (`VIRTUAL / SIMULATED`), exchange read connectivity, and live-trading authorization are separate states.
- Account reads never grant trading authority. The GUI provides no real exchange order-submit, cancel, withdrawal, transfer, liquidation, schedule-resume, or policy hard-delete path.
- Policy impact remains Preview-only. Builtin, historically referenced, and paper-session-referenced identities stay visible and protected.
- Run research notes are intentionally outside the product scope at the user's request. Any pre-existing `run-notes.json` is preserved without being read, overwritten, migrated, or deleted.
- Settings remain local and contain domain, port, authentication and financial MCP exposure configuration. Managed order-sending state is read from Rust and is never saved as an ON preference. There is no project-folder chooser.

## Current Gaps

- Persistent paper sessions use public completed-candle polling (`SIMULATED_SCENARIO` market rules and independent per-market virtual accounts); real-time WebSocket/SSE orderbook streaming, shared-capital paper portfolios, and an independent F08 paper verifier are not implemented and are explicitly labeled as such.
- Requested 2048×1235 viewport height is clamped by the local macOS display manager to 2048×1195 content height.
- Typed metric absence reasons are validated at the reader boundary but are not yet exposed in the history cell; the UI currently shows the broader `지표 미확인` state.

## Stack and Ownership

Swift/AppKit owns the native UI. The existing Rust backend remains the authority for MCP tools, control receipts, immutable research data, policy preview, exchange reads, and ledger detail. SQLite production data is read-only from the desktop; destructive verification uses neither the production ledger nor live trading effects.

## Verified history continuation

The native history catalogue supports combined KST creation-day, status, market, exact policy/revision, frozen-cost and pinned-run filters; compact/detail columns; max4 selection with visible hidden-selection disclosure and explicit clearing; independent selected-run comparison using the existing strategy cards; and exact selected-model return/drawdown ordering. CSV stores scope separately from observed facts.

The local history workspace persists filters, pins, selection, detail mode, metric sort, and last-opened run through an atomic bounded JSON write. Actual Mac restarts restored the H1 XRP policy/revision/cost/KST/pinned-only selection and metric sort. The current note-removal receipt and scoped independent review verify that these behaviors remain while the note button, controller, model, storage and tests are absent. The previous note evidence remains historical only. Current receipt: `.omx/evidence/p13/note-removal/note-removal-acceptance-receipt.json`; review: `.omx/evidence/p13/note-removal-independent-review.md`. The History module remains `PARTIAL` for complete 100+ native scroll/frame-time and keyboard/accessibility acceptance.
