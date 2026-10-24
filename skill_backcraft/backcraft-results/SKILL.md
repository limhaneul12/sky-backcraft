---
name: backcraft-results
description: Sky Backcraft의 기존 독립 모델·공유 포트폴리오·regime 결과와 비용·기여도를 조회하고 export 파일을 수신·검산한다. 결과 비교·파일 다운로드·검증 요청에 사용한다. 새 연구 실행이나 정책·데이터 변경은 시작하지 않는다.
---

# Backcraft 결과 분석·파일 수신

기존 실행을 정확히 설명하고, 요청받은 파일은 **클라이언트에 실제 수신한 뒤** 전달한다.
결과 분석만 요청받았다면 새 백테스트나 정책 수정을 시작하지 않는다.

## 실행 찾기와 비교

- 현재 MCP 도구/스키마를 확인한다. 주어진 `run_id`가 있으면 바로 조회한다. 없으면 `history_query(action="runs")`에서 찾고 후보가 여럿이면 날짜·시장·정책으로 좁힌다. 표시 이름이나 과거 예제 ID로 추정하지 않는다.
- 먼저 실행의 capital_mode와 종류를 확인한다. 독립 모델 실행은 `result_query({action:"summary", run_id})`, 공유 portfolio run은 `portfolio_summary`와 `portfolio_backtest(get)`을 사용한다. 공유 실행이 일반 runs 이력에서 안 보이면 `portfolio_backtest(list)`로 찾는다. 조회 종류를 바꾸기 위해 새 run을 만들지 않는다.
- 독립 summary의 `market`, `strategy`, `policy_ref`, `status`, `initial_cash`, `terminal_equity`, `total_return_ratio`, `total_fees`, `fill_count`, `closed_positions`, `open_positions`, 사유를 사용한다. family가 같아도 다른 revision/model은 별개다. 독립 모델의 기말자산을 합쳐 공유 포트폴리오 수익률로 보고하지 않는다. 공유 실행은 실제 한 cash pool의 결과를 읽는다.
- `total_return_ratio`는 비율이다. 퍼센트 표시는 ×100하고 원본 JSON 값과 단위를 보존한다. `null`과 `financial_null_reason`은 0/무손실로 바꾸지 않는다. 반대로 실제 완료됐지만 매매가 없었던 모델의 0 수익률은 차단 모델과 구분한다.
- 상세 확인이 필요할 때만 `result_query(action="costs", run_id, model_id)` 또는 `action="ledger"`를 사용한다. ledger 인자는 `query` 안에 넣는다:

```text
{action:"ledger", query:{run_id, model_id, section:"episodes", range:null, cursor:null, limit:100}}
```

`signals`, `orders`, `order_events`, `fills`, `episodes`, `equity` 중 필요한 section만
읽고 반환된 cursor를 그대로 이어간다. 한 페이지나 잘린 출력으로 전체 건수를 추정하지 않는다.
화면용 요약 곡선이 필요할 때는 `result_query({action:"equity_chart", run_id, model_id, max_points:256})`
(`max_points` 2–1000, 직렬화 상한 352 KiB 초과 시 극값 보존 자동 다운샘플링)을 사용한다.
전체 원장이 필요하면 페이지 조회로 재구성하지 말고 export 파일을 수신한다.

## 공유 포트폴리오·Regime 결과

- 요약: `result_query({action:"portfolio_summary",run_id})`의 initial_cash/terminal_equity/total_return/max_drawdown/cash_weight/turnover/fees/embedded_cost/portfolio_pnl을 읽는다. ratio 표시는 원본 단위를 보존한다.
- equity/allocations/rebalances와 regime_timeline은 `{action:"portfolio_equity",run_id,offset:0,limit:100}` 형식으로 필요한 action만 조회한다. `data.next_offset`을 그대로 이어가며 total_count와 반환 행 수를 구분한다. 현재 projection 페이지 상한은 500이며 일반 model-ledger 한도와 다르다.
- 모든 portfolio/regime projection은 `{status:"AVAILABLE",data:...}` 또는 `{status:"UNAVAILABLE",reason:...}`다. LEGACY_PROJECTION_MISSING이나 REGIME_NOT_CONFIGURED를 0 수익률·빈 성공으로 바꾸지 않는다. 기존 facts는 `portfolio_backtest(action="facts",run_id,kind,offset,limit)`로 확인할 수 있다.
- `portfolio_contributions`의 market와 policy는 같은 경제 손익의 두 분해 축이다. **market net_pnl 합 + residual = portfolio_pnl**을 Decimal로 응답의 tolerance(현재 0.00000001) 안에서 검산한다. 두 축을 합산하지 않으며 이미 net에 포함된 fees/embedded_cost를 다시 차감하지 않는다.
- cash_drag=0은 명목 no-yield cash 회계다. 미관측 기회비용이 없다는 투자 결론이나 benchmark 대비 cash 손실로 해석하지 않는다. MDD는 관측된 equity marks 기준이다.
- `regime_summary`에서 label별 time/PnL/MDD/trades/turnover/cost/transitions, `regime_timeline`에서 시장별 previous/current/time/reason/feature reference를 읽는다. UNKNOWN은 warmup/분류 미완의 명시적 상태다. 사용자 gate가 DISABLED인지 확인하고 cash 동작을 설명한다. 서로 다른 classifier parameter/dataset/interval 결과를 같은 label 이름만으로 동등하게 비교하지 않는다.
- shared 결과를 구버전 model export와 혼동하지 않는다. 요청한 run 종류의 export가 실제 제공되는지 확인하고, 없다면 지원되는 공개 projection을 전달하며 전체 ledger 파일을 받은 것으로 보고하지 않는다.

## 비용·청산 해석

- 원장 fill의 기존 `price_cost_attribution`은 **KRW/기준자산 1단위** 차이값이다. 최신 MCP fill의 `price_difference_per_unit`, `price_cost_attribution_unit`, `embedded_price_cost_quote`를 함께 읽는다.
- 실제 금액 기준 비용은 단위가격 차이 × 체결수량이다. 예: `43000 KRW/BTC × 0.07028686 BTC = 3022.33498 KRW`. float 대신 Decimal로 검산한다. 유리한 체결의 음수도 임의로 0으로 보정하지 않는다.
- 최신 costs 응답의 `price_cost_attribution_unit=KRW`와 review의 cost schema/unit을 확인한다. 이 가격 비용은 이미 체결가격에 반영되어 있으므로 현금·손익에서 다시 차감하지 않는다. 구버전 review의 단위가격 합계를 최신 KRW 집계로 오해하지 않는다.
- episode의 기존 `exit_reason`은 실행 결과를 담는 레거시 필드명이다. 최신 응답의 `exit_details.strategy_exit_reasons`와 `exit_details.execution_result`를 분리해 제시한다. 원본 신호 이유를 임의의 자연어 전략 조건으로 바꾸지 않는다.
- 미청산 episode는 종료 사유가 없다. 강제 기말청산 `ARTIFICIAL_TERMINAL_EXIT`은 전략이 발동한 청산으로 세지 않는다.

## 전체 파일 받기

1. 이미 원하는 export가 있으면 해당 export job이나 `artifact_query({action:"list",run_id})`로 메타데이터를 찾는다. 같은 파일명이라도 export 버전·범위·artifact ID가 다를 수 있으므로 섞지 않는다. export job ID가 없으면 `history_query(action="jobs")`의 EXPORT 후보를 `job_control(get)`으로 열어 요청한 run·market 범위와 `artifacts[].run_id`가 일치하는지 확인한다. job 목록의 이름이나 시각만으로 수신 대상을 고르지 않는다.
2. 새 export가 요청 범위에 필요하면 `export_report`에 request_id/run_id와 요청 범위(전체는 `market:null`, 특정 종목은 해당 market)를 넣어 제출하고 `job_control(get)`으로 terminal 상태를 확인한다. 제출 직후 artifact ID만 받았다고 다운로드 완료라 하지 않는다.
3. 완료 응답의 최상위 `artifacts`에서 파일명·형식·크기·SHA-256과 `retrieval.tool_name/read_arguments`를 얻는다. 서버 로컬 경로는 다운로드 링크가 아니다.
4. 저장소와 실행 가능한 클라이언트가 있으면 기존 수신기를 재사용한다. 아래 명령은 **저장소 루트에서** 실행하며, URL은 현재 연결의 실제 URL, 출력은 새 클라이언트 디렉터리로 바꾼다:

```sh
python3 scripts/python/download-export.py \
  --mcp-url "$BACKCRAFT_MCP_URL" \
  --job-id "$BACKCRAFT_EXPORT_JOB_ID" \
  --output "$BACKCRAFT_RECEIVE_DIR"
```

주소를 과거 Quick Tunnel 값으로 고정하지 않는다. 수신기는 표준 라이브러리만 쓰고
기존 파일을 덮어쓰지 않는다. 완료 export job의 파일 집합을 함께 받아 manifest와
review/ledger의 대응 관계를 유지한다. v4에서는 job으로 manifest만 먼저 수신하고
manifest의 정확한 ID·해시로 나머지 파일을 읽는다. 구버전 수신의
`manifest_references_followed=false` 경고를 성공적인 manifest 참조 검증으로 보고하지
않는다. 교정된 참조가 필요하면 원본을 보존한 새 export를 사용한다.

수신기가 없는 환경에서는 `retrieval.read_arguments`로 `artifact_query`를 호출한다.
`HEX`를 바이트로 디코딩하고 offset·chunk 크기·chunk SHA를 검증하며 `next_offset`을
끝까지 따른다. 마지막에는 전체 bytes/SHA와 제공된 gzip 해제 후 bytes/SHA도 확인한다.
큰 HEX를 답변에 붙이지 않는다. 파일로 쓸 실행 환경이 없으면 수신을 완료했다고 하지 말고
확인한 메타데이터와 호출 인자, 파일 저장에 필요한 환경을 명확히 전달한다.

## 자동매매·실거래 복기

### Paper의 수동 JSON 복기

- 현재 도구 스키마에 `review_events`가 있을 때
  `paper_session_query({action:"review_events",session_id,after_sequence:null,as_of_sequence:null,limit:50})`로
  시작한다. 반환 `as_of_sequence`를 고정하고 `page.next_cursor.sequence`를
  `after_sequence`로 보내 다음 페이지를 읽는다. 제한은 1–100행과 원본 이벤트
  JSON 128 KiB다. RESOURCE_LIMIT이면 limit을 줄이고, 단일 행도 초과하면 미수신으로 남긴다.
- `page.total_count`는 cutoff까지 전체 prefix, `returned_count`는 이번 페이지다.
  중간 cursor부터 시작한 마지막 페이지를 전체 이력으로 보고하지 않는다.
  종목·모델·판단을 시간축만으로 합치지 않고 원본 outcome/reasons/상태/목표를 유지한다.
- `configuration`은 원래 실행 설정이다. policy_ref의 정확한 ID/revision/digest로
  `policy_query(get)`을 연결하고 현재 head나 코인/전략 이름으로 정책을 추정하지 않는다.
  원본 AST의 AND/OR/NOT·규칙 순서와 Decimal/f64·단위를 그대로 보존한다.
- 현재 lifecycle·freshness·제한된 회계 검증은
  `paper_session_query({action:"request",request_id:configuration.request_id})`로 읽는다.
  found/session_id/policy_ref를 확인하고 `session.sequence`가 cutoff와 같을 때만
  그 현재 검증 결과를 해당 prefix에 연결한다. 다르면 검증 범위를 분리한다.
- 기존 `list/get/events`는 polling/reconciliation으로 원장을 진행시킬 수 있다.
  순수 복기 요청에 대한 fallback으로 호출하지 않는다. 읽기 전용 전용 도구인
  `paper_review_query`(`list`/`get`/`events`/`request`/`evaluations`/`decision`)가 제공되면
  원장 부작용 없이 세션 목록·상세·이벤트·요청 영수증·조건별 평가(`evaluations`)를
  조회할 수 있다. `review_events`와 `paper_review_query`가 모두 없으면
  기존 Request·정책 조회의 확인 범위와 새 서버/스키마 배포 필요성을 보고한다.
- 저장된 조건별 평가 trace가 없으면 UNKNOWN/NOT_CAPTURED이며 FALSE 또는
  NOT_EVALUATED라고 추정하지 않는다. 사후 값 비교는 저장 snapshot에서의 재구성으로
  표시한다. 회계/event/reference PASS를 전략·지표·공급자 최초 관측·수익성 검증으로
  확대하지 않는다. 후보 중복 규칙/비교 결과가 없으면 기회 수와 가상 손익은 미제공이다.

관련 계약과 구분된 예제는 [읽기 전용 JSON 복기](../../docs/json-review.md)와 [확장 JSON 복기 계약](../../docs/json-review-extensions-2026-10-20.md)을 사용한다.

연구 run과 자동매매 session_id를 구분한다. private `trading:read`가 있는 경우
`automation_session_query`의 decisions/decision/performance로 저장된 당시
캔들·지표·이유와 원장을 읽는다. LIVE/MOCK·동결 정책 버전·원가/평가 근거와
null 사유를 보존한다. 현재 시세로 과거 판단을 재실행하거나 현금 흐름을 수익으로
표시하지 않는다. export_manifest/export_read로 6개 CSV/JSON/Markdown 파일을
받아 snapshot·파일·청크 해시와 전체 길이를 검증한다. 연구 artifact_query에는
이 private 파일이 없다. [거래·복기 계약](../../docs/live-trading-mcp.md)을 따른다.

복기/다운로드 요청은 live_order_control enable 또는 거래 시작 승인이 아니다.
전송이 OFF여도 기록을 조회할 수 있다. 단순 결과 조회를 위해 ON으로 바꾸지 않는다.

## 검산, 이력, 보고

- 독립 검산 요청은 `verify_run({request_id,artifact_id,replay:false})`로 제출하고 완료된 report의 status/findings를 확인한다. 재생 요청에만 `replay:true`를 사용한다. artifact ID는 해당 패키지에서 실제 반환된 것을 쓴다.
- 로컬 파일은 `spot-lab verify-export --directory <수신 디렉터리>`로 검산한다. `replay-export`는 기록된 source/lock/toolchain/engine 조건을 충족해야 한다. 불일치를 숨기거나 버전 검사를 끄지 않는다. 원래 소스의 재생과 현재 소스의 독립 검산을 구분한다.
- 기존 파일·원장·run 해시는 보존한다. 구버전 숫자를 원본 파일에 덮어써 고치지 않는다. 비교 보고에는 단위·schema와 수정 전후 의미를 함께 적는다.
- `cause=UNKNOWN`에 `OFFICIAL_COMPLETED_MAINTENANCE_MATCH`가 붙어도 실제 중단 시작이 확인된 것은 아니다. 근거와 불확실성을 함께 표시하고, 봉을 보간하거나 차단을 풀지 않는다.
- `lab_status`의 구현 라벨, source-bound verification_receipt, CI 성공, 파일 해시 일치, 독립 검산, 재생 성공은 서로 다른 증거다. VERIFIED는 현재 응답의 code_revision/source/lock/schema/engine/toolchain에 묶인 빌드 검증이며 수익성이나 소비자 E2E 증거가 아니다. receipt 없는 IMPLEMENTED_UNVERIFIED와 기능 부재도 구분한다. 실제 확인한 단계만 성공으로 보고한다.

최종 답변에는 비교 요약, run/정책 revision 식별자, null/차단 사유, 수행한 검증,
실제로 저장된 파일 링크와 해시를 필요한 만큼 제공한다. 파일이 없으면 가짜 링크를 만들지 않는다.

## 엔진이 기록한 매수·미매수 이유

실제 도구 스키마와 저장 응답을 먼저 확인한다. Paper는 `paper_review_query`의
`decision`에 `selection:{session_id,event_sequence,as_of_sequence,signal_id}`를
넣고 선택한 원본 signal ID와 inclusive cutoff를 고정한다. `signal_id`는 해당
이벤트에서 실제 반환된 값만 사용하며, 신호가 여러 개면 명시적으로 선택한다.
Automation은 `automation_session_query(action="decision",session_id,intent_id)`를 사용한다.

`recorded_decision`의 실제 비중·원시/제약 후 목표 비중·조건별 operand/operator와
평가 결과를 근거로 전략 판단, 자금/비중 제약, 승인 대기, 주문/체결 결과를 구분한다.
방문하지 않은 조건은 NOT_EVALUATED, 필요한 값 부족은 UNKNOWN, 과거 미기록은
NOT_CAPTURED다. 기록이 없다는 이유로 FALSE나 HOLD로 바꾸지 않는다. 이후 체결은
선택 cutoff 안의 정확히 연결된 사실만 반영한다. 준비 대기/승인 대기의
DECISION_DEFERRED는 새 판단·거절·알려진 거래소 체결을 뜻하지 않는다.

V1/V2 backtest는 기존 model-ledger 직렬화를 유지하며 새 diagnostic capture가 없다.
V3·paper·automation의 신규 기록과 과거 누락을 구분한다. 매수 이유는 엔진이
원래 평가 때 남긴 사실만 설명하며 LLM 복기·재평가로 채우거나 수익성 증명으로
확대하지 않는다. 서버가 구버전이면 필드 미제공과 배포 필요성을 명시한다.

계약: [판단·입력 보호](../../docs/recorded-decisions-and-input-lifetime-2026-10-24.md),
[판단 설명 스키마](../../docs/schema/recorded-decision-explanation.json),
[준비 대기 스키마](../../docs/schema/decision-preparation.json),
[승인 대기 스키마](../../docs/schema/admission-deferred-fact.json),
[paper selection 스키마](../../docs/schema/paper-review-query.json).

## 필요한 자료만 읽기

저장소 밖에서는 상대 경로 대신 실제 MCP 스키마를 확인한다.

- 필드 해석: [데이터 사전](../../docs/data-dictionary.md), [result-query 스키마](../../docs/schema/mcp-result-query.json).
- 공유 회계: [summary](../../docs/schema/portfolio-result-summary.json), [contributions](../../docs/schema/portfolio-contributions.json), [regime timeline](../../docs/schema/portfolio-regime-timeline.json), [regime summary](../../docs/schema/portfolio-regime-summary.json).
- 수신·작업: [운영 가이드](../../docs/operator-guide.md), [수신기](../../scripts/python/download-export.py), [artifact 응답 스키마](../../docs/schema/artifact-query-result.json).
- 점검 근거: [공백 분류 기준](../../docs/upbit-maintenance.md).
