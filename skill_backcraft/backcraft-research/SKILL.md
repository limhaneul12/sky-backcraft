---
name: backcraft-research
description: Sky Backcraft에서 파라미터 스윕으로 정책 후보를 일괄 생성하고, 리서치 스위트(walk-forward/batch)를 사전 계획·실행하며, 공유자본 포트폴리오와 regime 게이팅 연구를 수행한다. 견고성·regime 연구 요청에 사용한다. 단순 백테스트 실행은 backcraft-experiment를, 결과 다운로드는 backcraft-results를 사용한다.
---

# Backcraft 리서치 스위트

견고성(robustness)과 regime 연구를 **반증 가능한 형태**로 수행한다. 모든 실행은
업비트 공개 데이터 기반의 모의 실험이며, long/cash만 지원하고 실제 주문은 없다.
요청한 단계까지만 수행한다. 계획(plan)만 요청받으면 create로 이어가지 않는다.

## 접속과 요청 확인

- 연결된 MCP에서 실제 도구 이름을 확인하고 `lab_status`로 한도를 읽는다.
  임시 터널 주소, 과거 run/dataset/policy ID를 지어내지 않는다.
- research suite는 **v3 템플릿**(`schema_version: "3.0"`, `strategies: []`,
  `policy_selections: [정확한 참조들]`, STRICT_PIT, 선언된 정책 warmup)만 받는다.
  금액·비중·bps는 decimal 문자열로 넣는다.

## 1. 파라미터 스윕 (정책 후보 일괄 생성)

1. `policy_write`에 `action: "sweep"`으로 `request_id`, `family`(예: S2),
   `template`(StrategySpec), `mode`를 넣는다.
   - tuples: `[{entry_length: 20, exit_length: 10}, ...]` — 명시 나열(권장)
   - grid: `{entry_length: [20,40,55], exit_length: [10,20]}` — 최대 32 후보
2. 반환된 `PolicySweepResult`에서 `revisions[].reference`
   (`{policy_id, revision_id, definition_digest}`) 전체를 보관한다. 이것이
   스위트 템플릿의 `policy_selections`가 된다.
3. 동일 파라미터셋은 기존 정책을 재사용하고 중복 tuple은 억제된다
   (`duplicates_suppressed`). 사용자 정의 파라미터는 익명으로 실행하지 않는다.

## 2. 리서치 스위트 — 반드시 plan 먼저

1. `research_suite`에 `action: "plan"`으로 요청을 넣고 보고서를 읽는다.
   `candidates/markets/folds/cost_scenarios/planned_runs/comparison_cells/
   estimated_run_events/estimated_storage/admission/suggestions`가 나온다.
   실행 한도: runs 64, comparison cells 256, folds 12, scenarios 4,
   aggregate run events 100,000.
2. `admission: rejected`면 숫자로 초과 이유를 사용자에게 보여주고
   `suggestions`를 따라 분할을 제안한다. **마켓 분할은 선택 의미가
   보존**되지만, candidate 분할·시나리오 축소는 selection 집계가 바뀌므로
   warning을 그대로 전달하고 사용자 승인을 받는다. 자동으로 연구 의미를
   바꾸지 않는다.
3. 승인되면 같은 요청으로 `action: "create"`를 실행한다. 템플릿에 dynamic
   cost model(`costs.dynamic`)이 있으면 cost sweep과 조합할 수 없다(create가
   거절한다) — 고정 모델로 스윕하거나 스윕을 생략한다.
4. 진행 조회: `get`(요약: status/completed_runs/selected_folds), `cases`,
   `comparisons`(fold·scenario·후보별 net_pnl/MDD/turnover), `pause`,
   `resume`. walk-forward는 fold마다 selection으로 winner를 확정한 뒤
   evaluation을 실행한다. winner가 확정된 fold의 evaluation은 winner 변경 없이
   재해석한다.

## 3. 공유자본 포트폴리오 (하나의 현금 풀)

1. 먼저 `plan_backtest`로 평가 범위를 포함한 plan을 freeze한다. 포트폴리오는
   **마켓당 정확히 하나의 admitted 전략**을 요구하므로 plan의 markets와
   포트폴리오 assets를 일치시킨다.
2. `portfolio_backtest`에 `action: "create"`로 넣는다:
   - `request_id`, `plan_id`, `input_digest`(plan 반환값 그대로)
   - `portfolio`: `initial_cash`, `assets[].market/max_weight`, 
     `risk{max_gross_exposure, min_cash_weight, drawdown_stop}`,
     `arbitration`(기본 PRIORITY, PRO_RATA/SCORE_RANKED 선택)
   - 제약: gross + reserve ≤ 1, `latency_ms` 0, 수동(passive) 체결 정책 미지원
3. 선택으로 `regime` 게이트를 넣는다: `classifier`(revision 문자열,
   sma_long/sma_mid/sma_short, slope/vol/atr lookback, 선택적
   high/low_vol_annualized)와 `rules`(trend_up/trend_down/chop/unknown 각각
   Enabled | Disabled | ReducedExposure). warmup 미완 구간은 unknown 규칙이
   적용된다. 게이트는 전략 on/off와 exposure cap만 하고 임의 주문을 만들지
   않는다.
4. 결과 조회: `get`(요약: status, totals — terminal_equity/total_return/
   max_drawdown/turnover/total_fees/price_cost_drag/exposure_seconds/
   rejected_signals/rejection_reasons — 과 benchmarks: CASH, 자산별
   B&H, static allocation), `facts`(kind: intent/fill/rejection/mark,
   offset/limit 페이지), `list`.

## 4. 해석 수칙

- **total return 하나로 성공 판단 금지.** MDD 감소, turnover, rejected
  signal 이유 분포, benchmark 대비 excess를 함께 본다.
- 거절된 신호는 실패가 아니라 자본 제약의 기록이다. `rejection` facts의
  이유(INSUFFICIENT_CASH, GROSS_EXPOSURE_CAP 등)로 자본 배분 병목을 읽는다.
- regime 게이팅 비교는 always-on vs 게이팅을 같은 기간·비용으로 나란히
  실행해 return/MDD/turnover를 나란히 제시한다.
- 시장규칙이 EXPLICIT_SCENARIO면 결과 해석 전에 미검증 가정임을 밝힌다.
  모든 결과는 SIMULATED_ONLY 모의 실행이다.

## 5. 안전 수칙

- 실제 주문·계좌 접근·private API 요구는 하지 않는다.
- 같은 데이터 루트의 실행 중 서버가 있으면 별도 CLI로 루트를 열거나 SQLite를
  직접 수정하지 않는다.
- 사용자가 이미 지정한 기간·시장·파라미터는 재확인하지 않는다. 누락된 필수
  입력(범위·마켓·정책 참조·자본·제약)만 묻는다.
- 요청 범위 밖의 기능(L2 orderbook, short, 실제 주문, ML 분류기)을 임의로
  추가하지 않는다.
