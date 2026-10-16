---
name: Sky Backcraft Native Research Studio
description: A calm native macOS workspace for exact strategy evidence and fail-closed operational observation.
colors:
  surface: "light-dark(#f6f6f6, #1b1b1b)"
  panel: "light-dark(#ffffff, #242424)"
  chart: "light-dark(#ffffff, #1b1b1b)"
  text-primary: "CanvasText"
  text-secondary: "light-dark(#4d4d4d, #c2c2c2)"
  accent: "AccentColor"
  positive: "light-dark(#146e42, #7ad9a3)"
  negative: "light-dark(#b32417, #ffaba1)"
typography:
  title:
    fontFamily: "-apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "20px"
    fontWeight: 600
  body:
    fontFamily: "-apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "14px"
    fontWeight: 400
  table:
    fontFamily: "-apple-system, BlinkMacSystemFont, sans-serif"
    fontSize: "13px"
    fontWeight: 400
    fontFeature: "tabular-nums"
rounded:
  panel: "6px"
spacing:
  xs: "5px"
  sm: "8px"
  control: "10px"
  panel: "14px"
  page: "20px"
components:
  panel:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.text-primary}"
    rounded: "{rounded.panel}"
    padding: "14px"
  metric:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.text-primary}"
    typography: "{typography.table}"
    rounded: "{rounded.panel}"
    padding: "8px 10px"
---

# Design System: Sky Backcraft Native Research Studio

## Overview

**Creative North Star: "Native Research Studio"**

Sky Backcraft is an Operate-mode macOS workspace: calm enough for long research sessions, dense enough to compare evidence, and explicit about every state that can affect financial interpretation. Native AppKit controls, semantic appearance colors, exact identities, and readable status copy provide trust. The research dashboard and the Settings window use one visual language rather than separate brown and navy themes.

The product is strategy-first. A policy is selected by exact immutable identity, while every run/model keeps its own market, window, cost assumption, capital mode, and ledger. Graphs may sit beside one another for comparison, but the interface never merges incompatible periods or averages unrelated results. Persistent paper, exchange reads, and trading authority remain distinct even when they share one app shell.

**Key characteristics:**

- Native AppKit sidebar, split views, tables, popups, segmented controls, sheets, menus, and focus behavior.
- Semantic light/dark surfaces with restrained positive/negative color; words and signs carry meaning before color.
- Medium-density research layouts with exact Decimal values and selectable immutable IDs.
- A 1200-wide focused workspace and a 1440-plus evidence workspace with a persistent inspector.
- Honest loading, empty, unsupported, failed, stale, and confirmed-zero states.

Current scope boundaries are part of the design truth: persistent paper sessions run on public completed-candle polling with `SIMULATED_SCENARIO` rules and independent per-market virtual accounts (while real-time WebSocket/SSE streaming, shared-capital paper portfolios, and an independent F08 paper verifier are explicitly marked unsupported).

## Colors

The palette is native and semantic. AppKit appearance resolves surfaces and text at render time; fixed colors are used only for meaning-bearing financial accents.

### Primary

- **System Accent** (`controlAccentColor` / `AccentColor`): selection, focused controls, and the SF Symbol brand mark. It does not encode profit, connection, or authorization.

### Neutral

- **Studio Surface** (`#f6f6f6` light / `#1b1b1b` dark): window and page background.
- **Evidence Panel** (`#ffffff` light / `#242424` dark): sidebar, cards, inspectors, and read-only detail regions.
- **Chart Ground** (`#ffffff` light / `#1b1b1b` dark): charts and tables.
- **Primary Text** (`labelColor` / `CanvasText`): titles, values, and actionable labels.
- **Secondary Text** (`#4d4d4d` light / `#c2c2c2` dark): descriptions, context, provenance hints, and timestamps.
- **Boundary** (`separatorColor`): table rules, split dividers, and panel borders.

### State colors

- **Verified Positive** (`#146e42` light / `#7ad9a3` dark): positive financial values only, always paired with a plus sign or words.
- **Verified Negative** (`#b32417` light / `#ffaba1` dark): negative values and errors, always paired with a minus sign or explicit status.
- **System Orange/Red:** warning and emergency states in Settings. Connectivity green never grants trading authority.

**The Evidence Before Color Rule.** Color reinforces a state; it never replaces the state name, exact number, source, or authorization boundary.

## Typography

**Display and Body Font:** the macOS system font through AppKit.
**Numeric Font:** `monospacedDigitSystemFont` for KPIs, money, percentages, counts, and table values.

The type system is compact and familiar. Page titles use 20 pt semibold, section titles generally use 17–18 pt semibold/bold, body copy uses 14 pt, context and metadata use 11–13 pt, and financial tables use 13 pt tabular figures. Long IDs are selectable or copyable and appear in full inside inspectors, tooltips, or detail panes rather than forcing wide primary columns.

**The Number Integrity Rule.** Display formatting may round for scanning, but tooltips, inspectors, ledger details, and CSV exports preserve the exact Decimal source.

## Layout

The primary window opens at 1200×820 and has a 1200×800 minimum content size. A vertical split owns a 230 pt sidebar, resizable from 180 to 280 pt, and the remaining workspace. A persistent top context rail summarizes historical research, paper capability, account-read state, and trading authority.

The seven sidebar destinations are `성과 분석`, `실행 이력`, `정책 카탈로그`, `지속 모의 · 작업`, `전략 자동화 · MOCK`, `실계좌 및 주문 조회`, and `삭제 영향 미리보기`. At `성과 분석`, the first view is a strategy browser: exact policy selector, market filter, equity/drawdown mode, evidence summary, and paged independent cards. Cards use two columns when their content region is at least 760 pt wide and one column below that threshold.

Opening a result switches to the immutable run/model workspace. Its order is actions and selectors → compact context → three primary KPIs → secondary cost/trade/capital facts → chart → model comparison. At 1440 pt and wider, a 270 pt evidence inspector opens by default. At 1200 pt it collapses to protect the chart and essential columns and remains available through `근거 패널`.

The policy catalogue is a native split view with a minimum 390 pt list and 470 pt detail area. The ledger-detail window opens at 1120×760 with a 920×640 minimum and splits paged records from selectable exact detail. Settings is a separate resizable 680×700 window. Review mode exposes exact 1200×800, 1440×900, and 2048×1235 viewport actions.

Spacing uses the observed 5/8/10/14/20 pt rhythm. Tables keep 32 pt rows, native scrolling, visible headers, and resizable or intentionally fixed essential columns. Horizontal scrolling is a fallback for exact evidence, not the primary way to read core KPIs.

## Elevation & Depth

The system is flat and layered by tone. Window surface, evidence panels, chart ground, split dividers, and one-pixel semantic borders establish hierarchy. It does not use decorative shadows, neon glows, glass cards, gradients, or simulated web elevation. Native window chrome, sheets, popovers, focus rings, and selection highlights provide transient depth.

**The Flat Evidence Rule.** A raised-looking element must represent native interaction state or a true foreground window, never decoration.

## Shapes

AppKit controls retain platform geometry. Custom evidence panels and metric containers use a restrained 6 pt radius with a one-pixel semantic border. Charts clip to their real bounds. Square table regions and thin split dividers preserve density; large pill controls and oversized card radii are avoided.

## Components

### Sidebar and global context

- The sidebar uses the `chart.xyaxis.line` SF Symbol, system selection, 36 pt rows, and a small ledger/run/policy count footer.
- The global context rail states research source, persistent-paper support, account read state, and live-trading authority in words.

### Strategy performance browser

- Policy selection is keyed by exact policy ID; duplicate names receive an identity suffix.
- Market filtering changes visibility only. Each card remains one immutable run/model with its own period, revision, cost, capital mode, KPI set, and chart.
- Equity and drawdown share the same verified mark source. Missing summaries and failed runs remain separately accessible under `성과 없는 실행`.
- CSV export covers the selected strategy/market scope, including unavailable values as unavailable rather than zero.

### Immutable run detail

- Primary KPIs are cumulative return, maximum drawdown, and net PnL. Fees, fills/closed episodes, and initial→final equity form a compact secondary row.
- Chart controls provide equity/drawdown switching, zoom, reset, and confirmed fill markers. Arrow keys inspect chart points.
- Model-row selection synchronizes the chart, selector, metrics, inspector, and ledger action.
- The inspector exposes F08 integrity, marks, window, costs, exact PnL/fees, run/model/plan/dataset/policy revision, engine, code revision, and raw state.

### Tables and history

- Essential columns fit first; long IDs move to details, tooltips, and copy actions.
- Sort state is visible on supported headers and selection identity survives sorting.
- History provides search, KST date, state, market, exact policy/revision, cost, and pinned-only filters; bounded pagination; compact/detail columns; selected-run provenance; explicit `성과에서 열기`; and filtered-scope CSV export.
- Up to four selected runs feed the existing independent comparison cards. Metric ordering is enabled only for an exact policy, market, and optional revision that resolves to one model per run; unavailable and ambiguous rows remain visible below known Decimal values.
- UI workspace state persists filters, pins, selected runs, compact/detail mode, metric sort, and last-opened run without writing to the research ledger.
- Missing-model, missing-metric, and ambiguous-model states use distinct labels. The underlying typed metric absence reason is not yet shown and remains a bounded P2 follow-up.
- Do not expose a research-note editor. The user removed that workflow from product scope; any existing note file is preserved as external legacy data without read or write behavior.
- The final native History surface has no note affordance and retains the exact H1 XRP revision/date/pin/selection/drawdown state; this removal is verified in the current note-removal receipt.

### Policy catalogue

- Search and origin/use/market filters preserve exact selection identity even when a selected row becomes hidden.
- The detail side shows immutable definition JSON, revision history, related ledger runs, and actions to open a run or view the exact policy in performance analysis.
- `HISTORICAL_ONLY` and `UNUSED` are evidence states. Registration, paused collection, and past use never become an active-session badge.

### Cost and ledger detail

- A separate read-only native window shows exact buy/sell notional, explicit fees, embedded execution-price diagnostic cost, terminal equity, and terminal mark sequence.
- Fills, orders, order events, episodes, and signals are paged, sortable where supported, selectable, and paired with readable detail plus exact original fields.
- The window states that this is historical modeled execution, not a live order surface.

### Strategy automation

The separate `전략 자동화 · MOCK` workspace identifies the frozen policy, market, cadence, explicit risk limits and server-owned lifecycle. Balances are masked by default. It pages owned orders, unique fills and recovery events; unknown outcomes keep their exact request identity and block new writes until durable readback. MOCK assumptions are visible; production order sending stays OFF. Pause/stop does not cancel orders or liquidate holdings.

### Live observation, Settings, and Preview

- Account balances are masked by default and explicitly refreshed. Zero results and read failures use different copy.
- Start, Resume, and Pause stay disabled with trading OFF. Emergency Stop names its exact effect: block admission of new orders, without implying cancel or liquidation.
- `지속 모의 · 작업` displays a prominent `VIRTUAL / SIMULATED · 실주문 없음 · 확정 봉(Completed Candle) 폴링` badge, frozen policy revision and `SIMULATED_SCENARIO` cost/rule creation form, `Create & Start` / `Pause` / `Resume` / `Stop` controls with `expected_sequence` CAS and unknown-receipt readback recovery, independent per-market virtual account rows (`INDEPENDENT_MARKET_ACCOUNTS`), paged virtual event logs, and the background job queue while keeping existing `PAUSED` collection schedules distinct from active paper sessions.
- Policy management is titled `삭제 영향 미리보기`; selection produces a server Preview sheet only. No hard-delete control is present.

## Do's and Don'ts

### Do

- **Do** keep exact policy, run, model, revision, dataset, market, window, cost, and capital identity together.
- **Do** use AppKit semantic colors, system controls, SF Symbols, focus rings, and selectable text.
- **Do** preserve last trustworthy values while labeling unavailable or failed reads explicitly.
- **Do** collapse the inspector at 1200 pt and open it at 1440 pt or wider without losing the current selection.
- **Do** show unsupported capability honestly and route users toward available historical evidence.

### Don't

- **Don't** average or connect results from different policies, markets, windows, costs, or capital modes.
- **Don't** turn F08 PASS, exchange connectivity, a completed replay, or a paused collector into a success or active-trading badge.
- **Don't** convert missing Decimal values to zero or export display-rounded values as source data.
- **Don't** expose order submission, cancellation, transfer, withdrawal, liquidation, schedule resume, or policy hard-delete through this GUI.
- **Don't** claim final accessibility, dark/light, viewport, performance, or persistent-paper acceptance until those specific gates have fresh evidence.
