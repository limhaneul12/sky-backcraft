---
name: backcraft-experiment
description: Sky Backcraft에서 업비트 데이터 수집·정기 수집, 정책 버전 관리와 멀티 타임프레임 백테스트를 계획·실행한다. 기존 결과 분석은 backcraft-results, 후보 스윕·워크포워드·공유자본 연구는 backcraft-research를 사용한다.
---

# Backcraft 실험 실행

요청한 정책·기간·가정을 그대로 동결해 재현 가능한 **모의 실험**을 만든다.
지원 범위는 Upbit 공개 데이터, 업비트 KRW 마켓 전체(요청 심볼은 실행 시 라이브 마켓 카탈로그로 검증), long/cash다. 이 스킬의 모의 실험 범위에는 실제 주문·계좌 접근이 포함되지 않는다.
실거래 전송·전략 세션 제어 요청은 [거래 제어 안내](../../docs/live-trading-mcp.md)의 별도 private MCP 경로를 따른다. 연구 실행 요청을 실주문 ON 승인으로 해석하지 않는다.
요청한 단계까지만 수행한다. 정책 저장만 요청받으면 수집·실행으로 이어가지 않는다.

## 접속과 요청 확인

- 연결된 MCP에서 실제 도구 이름과 입력 스키마를 확인하고 `lab_status`로 시장 범위, 실행 가능 상태와 한도를 읽는다. 플랫폼별 도구 접두사는 발견된 이름을 따른다.
- 소스 문서와 연결 서버의 버전은 다를 수 있다. `code_revision`, `source_digest`, `implementation_status`, `verification_receipt`를 읽고 실제 제공되는 action만 사용한다. `VERIFIED`는 해당 빌드의 검증 증거이지 수익성·현재 외부 서비스·소비자 연결 성공의 보증이 아니다. `IMPLEMENTED_UNVERIFIED`만으로 기능이 없다고 단정하지 않는다.
- 실행 중인 MCP가 있으면 이를 사용한다. 연결이 없으면 사용 가능한 연결 정보나 저장소 위치를 확인한다. 임시 터널 주소, 과거 run/dataset ID, API 키를 지어내지 않는다.
- 같은 데이터 루트를 소유한 서버가 실행 중일 때 별도 CLI 프로세스로 루트를 열거나 SQLite를 직접 수정하지 않는다.
- **실험 실행 시에만** 시장, UTC `[start,end)`, 데이터·판단·체결 주기, warmup, 정책 revision, 초기자본, 비용, 체결/종료 규칙과 시장규칙 가정을 확인한다. 사용자가 이미 지정한 값은 재확인하지 않는다. 누락된 필수 입력(기간·시장·주기·정책·경제적 가정 등)만 묻고, 명시적으로 시나리오 제안을 요청받았다면 가정과 역사적 미검증 여부를 구분한다.
- 금액·수량·비중·bps는 스키마에 맞는 decimal 문자열이다. KST 입력은 UTC로 변환하되 기간을 임의로 늘리거나 줄이지 않는다. 예제의 날짜·수수료·호가규칙을 현재 사실이나 사용자 설정으로 자동 채택하지 않는다.
- BTC/ETH/XRP 외 심볼은 사용자가 지정하거나 사용자와 합의한 경우에만 선택한다. 호가단위·수량단위·최소주문금액 같은 시장규칙은 실험 입력이므로, 소형 코인에서는 실제 규칙과 다른 가정이 체결 시뮬레이션의 신뢰도를 떨어뜨린다는 점을 결과 해석 전에 밝힌다.

## 정책 선택 또는 작성

1. `policy_query`의 `list`로 현재 head를 찾고 `get`으로 정확한 정의를 읽는다. 실행용 참조는 반환된 `{policy_id, revision_id, definition_digest}` 전체를 보관한다.
2. 기존 정책 실행만 요청받았다면 수정하지 않는다. 수정 요청은 전체 정의를 복사해 의도한 부분만 바꾸고 `policy_write(action="revise")`에 현재 `expected_parent_revision_id`와 새 `request_id`를 넣는다. 새 정책은 `action="create"`다.
3. 부모 revision 충돌이면 읽어 둔 원본·의도한 변경·최신 head를 비교한다. 필드와 의미가 독립적임을 확인한 변경만 최신 부모와 새 request ID로 다시 적용한다. 동일 필드나 연관 조건이 충돌하면 차이를 제시하고 사용자 선택을 받는다. 기존 revision과 과거 실행은 보존한다.
4. `BUILTIN` 매개변수 변경뿐 아니라 전체 프로그램을 `RULES`로 교체할 수 있다. family는 분류용 메타데이터이므로 이름이 S1이라고 실행 로직도 S1이라고 단정하지 않는다.
5. 사용자 정의 정책은 유한 JSON DSL이다. 지원하지 않는 로직을 임의의 Python/Rust/SQL 코드로 제출하지 않는다. 순서상 첫 조건이 선택되며 상태 갱신은 모두 이전 상태를 읽는다. `HOLD`는 실제 보유 비중 유지이지, 미체결 목표의 기억이 아니다. 필요한 기억은 명시적 state로 표현한다.

등록/수정 후 반환된 revision을 다시 확인한다. 선언된 정책 warmup을 쓰는 새 실험은
schema `3.0`, `strategies: []`, `policy_selections: [정확한 참조들]`,
`causal_execution: "DECLARED_POLICY_WARMUP"`, `pit_policy: "STRICT_PIT"`를 사용한다.
기존 v1/v2 재현 요청은 원래 계약을 유지한다. v2에는 `causal_execution`을 넣지 않는다.

## 멀티 타임프레임 정책

- indicator의 `source_interval`로 `m1/m5/m15/h1/h4/d1`을 지정할 수 있다. 예를 들어 H1 EMA20/50, M15 RSI14, M5 CLOSE/EMA20 조건을 M1에서 실행할 수 있다. 정확한 DSL은 [정의 스키마](../../docs/schema/policy-definition.json)를 따른다.
- source bar의 close가 decision time 이하인 완성 봉만 사용한다. native interval dataset이 있으면 사용하고, 없으면 지원되는 finer feed를 UTC 경계로 집계한다. constituent 누락·partial bar는 보간하지 않는다.
- 실행 범위와 각 source interval의 실제 warmup을 함께 확보한다. M1의 50개 봉이 H1 EMA50의 warmup을 충족한다고 계산하지 않는다. 데이터 한도가 부족하면 계획의 거절 이유와 범위 조정 조건을 전달한다.

## 데이터 → 계획 → 실행

- 먼저 `dataset_query`로 요청 범위·주기·warmup에 맞는 기존 스냅샷을 찾는다. 적합하지 않으면 `collect_data`에 명시적 `CollectRequest`를 제출한다. 수집 범위와 warmup을 합친 크기도 한도에 포함된다.
- 수집 작업의 terminal output에서 `dataset_id`를 얻어 coverage·quality를 확인한다. 점검 공지와 일치해도 `UNKNOWN` 또는 `BLOCKED_DATA`가 자동 해제되지는 않는다. 가격 보간, 시간 삭제, 가짜 봉으로 통과시키지 않는다.
- 여러 dataset을 한 계획에 넣을 때 같은 시장·같은 interval의 충돌하는 관측 중복을 피한다. 서로 다른 source interval의 같은 시간 범위는 MTF 입력일 수 있다. 파생 dataset에는 원본 dataset closure도 포함한다.
- 전체 `PlanRequest`를 `plan_backtest`에 제출한다. 예제의 schema와 사용하려는 실행 계약을 확인하고 revision 참조·warmup·필수 가정을 모두 채운다.
- `capital_mode`가 없거나 `INDEPENDENT_MODELS`면 `run_backtests`를 사용한다. 공유자본 연구는 계획에 `SHARED_PORTFOLIO`를 지정하고 [backcraft-research](../backcraft-research/SKILL.md)의 `portfolio_backtest` 경로를 사용한다. 독립 계정들을 합쳐 공유 포트폴리오로 간주하지 않는다.
- 계획의 `admissions`, `warnings`, `active_limits`, `request_size`를 읽는다. 한도 거절의 requested/allowed/excess와 줄일 조건을 전달한다. 분할·기간 축소·종목/정책 제외는 사용자 지시나 이미 합의한 범위에 따라 명시적으로 정하고 새 요청으로 남긴다.
- 기간을 여러 run으로 나누면 각 run이 현금과 초기 정책 상태에서 다시 시작하므로, 단일 연속 실험과 동등하다고 보고하지 않는다.
- Evidence가 없는 S5/coverage control의 차단은 수익률 0이 아니다. Evidence를 꾸며 넣지 않는다. 사용자가 요청한 실험에 차단 모델이 있어도 임의로 제외하지 않고 계획에 표시한다.
- 실행할 계획에서 반환된 `id`와 `input_digest`를 그대로 사용한다:

```text
run_backtests({request_id: 실행 요청 ID, plan_id: 계획.id, input_digest: 계획.input_digest})
job_control({action: "get", job_id: 반환된 job.id})
```

## 작업과 재시도

- `transport.progress_stream=false`이면 작업 진행은 `job_control` polling이다. 정기 캔들 수집과 실시간 호가·자동 push를 혼동하지 않는다.
- 제출 응답은 완료 증거가 아니다. `job_control(get)`으로 해당 attempt의 terminal 상태와 output을 확인한다. `PARTIAL`, `BLOCKED`, `FAILED`, `CANCELLED`, `INTERRUPTED`를 `COMPLETED`와 구분한다.
- polling은 연속 호출 대신 간격과 작업에 맞는 유한 관찰 시간을 둔다. 관찰 시간이 끝나면 job ID와 현재 상태를 남긴다. 사용자가 중단을 요청하지 않았다면 관찰 만료만으로 취소하지 않는다.
- `request_id`는 동일한 정규화 요청의 멱등성 키다. 내용을 바꾸면 새 ID를 사용한다. timeout/`OUTCOME_UNKNOWN`이면 기존 job 또는 history에서 durable 상태를 먼저 조회한다. 새 ID로 같은 작업을 중복 제출하지 않는다.
- `job_control(retry)`는 새 attempt를 만든다. 원인과 기존 상태를 확인한 뒤 사용자 요청 범위에서만 유한하게 재시도한다. `cancel`은 취소 요청일 뿐, terminal 확인 전 완료라고 말하지 않는다.
- 진행 건수는 `count_unit`과 함께 읽고 `null`을 0으로 바꾸지 않는다. 서버의 파일/큐/이벤트 제한을 우회하지 않는다.

## 정기 수집·신선도·복구

- 정기 수집을 요청받았을 때 `collection_schedule(action="create", request=...)`를 사용한다. request에는 시장·interval·lookback_bars·cadence_seconds·retry와 선택적 freshness_policy를 넣는다. 실제 schema/한도를 따르며 기본 cadence 하한은 60초다. 서버 runtime이 켜져 있을 때 완성 캔들을 수집한다.
- 기존 schedule 점검은 `list/get`, 신선도 확인은 `freshness`를 사용한다. 소스 지연과 수집기 지연을 구분해야 할 때만 `probe_source:true`로 실제 source를 조회한다. 단순 최신 boundary 누락을 바로 true gap으로 단정하지 않는다.
- durable `recovery_state`는 `active/degraded/recovery_wait/probing/backfilling/verifying_freshness/operator_required`다. `failure_class`, `recovery_attempt_count`, `last_probe_at`, `last_recovery_at`, `pending_gap`, `backfill_job_id`, `next_recovery_at`을 같이 읽는다. backfill job 완료만으로 복구 완료를 선언하지 말고 `active`와 시장별 latest/gap 상태를 확인한다.
- transient failure의 bounded backoff·backfill은 기존 scheduler가 소유한다. 별도 collect job을 중복 생성하거나 schedule 소유 job을 직접 retry/cancel하지 않는다. permanent 설정 오류는 원인 해결 후 요청 범위에 맞게 Resume한다.
- pause/resume 응답의 `mutation_outcome`을 읽는다. 응답 손실이나 `RECONCILIATION_REQUIRED`는 get으로 durable 상태를 먼저 확인한다. 새 schedule을 만들어 우회하지 않는다.

완료 보고는 수행한 단계에 맞춘다. 정책 작업이면 revision, 수집이면 dataset/job ID,
실험이면 run/job ID·revision·input digest·가정·완료/차단 모델 수를 남긴다.
실패/미확인 범위를 구분하고 수익성이 검증됐다는 결론을 만들지 않는다.

## 제출·재시도의 입력 검증

작업 제출·재시도 시 서버는 필요한 dataset·plan·raw·policy·evidence 입력을 검증하고
같은 Store 트랜잭션에서 작업을 등록한다. 실제 입력 오류를 무시하거나 빈 입력으로
바꾸어 재시도하지 않는다. Queued/Running 및 실행 취소 진행 중의 입력은 마지막
worker 사용 종료까지 삭제 보호를 받는다. 등록 뒤 입력이 사라지면 해당 시도는
명시적으로 실패하며 다른 정상 대기 작업의 실행을 막지 않는다. 구형 서버 배포와
스킬 갱신은 구분한다. [입력 보호 계약](../../docs/recorded-decisions-and-input-lifetime-2026-10-24.md)을 따른다.

## 필요한 자료만 읽기

아래 상대 경로는 이 저장소 안에서 제공된다. 다른 환경으로 스킬만 옮겼다면 실제 MCP
스키마를 확인하고, 필요한 문서/도구가 없으면 그 한계를 알린다.

- 요청 작성: [운영 가이드](../../docs/operator-guide.md), [plan-request 스키마](../../docs/schema/plan-request.json), [수집 예제](../../docs/examples/collect-h1.json), [전체 실험 예제](../../docs/examples/plan-h1.json).
- 사용자 정책: [정의 스키마](../../docs/schema/policy-definition.json), [EMA 교차 예제](../../docs/examples/policy-create-custom.json). 예제는 전략 추천이 아니다.
- 정기 수집: [action 스키마](../../docs/schema/collection-schedule-action.json), [record 스키마](../../docs/schema/collection-schedule-record.json), [freshness 응답](../../docs/schema/collection-freshness.json).
- 실행 후 비교·파일 수신이 필요하면 [backcraft-results](../backcraft-results/SKILL.md)를 읽는다.
