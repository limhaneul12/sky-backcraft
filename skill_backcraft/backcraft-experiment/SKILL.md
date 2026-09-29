---
name: backcraft-experiment
description: Sky Backcraft에서 업비트 데이터를 수집하고 S1~S5 또는 사용자 정의 정책의 버전을 저장·수정하여 백테스트를 계획·실행한다. 데이터 수집, 정책 작성, 실험 실행 요청에 사용한다. 기존 실행의 분석·다운로드만 필요하면 backcraft-results를 사용한다.
---

# Backcraft 실험 실행

요청한 정책·기간·가정을 그대로 동결해 재현 가능한 **모의 실험**을 만든다.
지원 범위는 Upbit 공개 데이터, KRW-BTC/ETH/XRP, long/cash다. 실제 주문·계좌 접근은 없다.
요청한 단계까지만 수행한다. 정책 저장만 요청받으면 수집·실행으로 이어가지 않는다.

## 접속과 요청 확인

- 연결된 MCP에서 실제 도구 이름과 입력 스키마를 확인하고 `lab_status`로 시장 범위, 실행 가능 상태와 한도를 읽는다. 플랫폼별 도구 접두사는 발견된 이름을 따른다.
- 실행 중인 MCP가 있으면 이를 사용한다. 연결이 없으면 사용 가능한 연결 정보나 저장소 위치를 확인한다. 임시 터널 주소, 과거 run/dataset ID, API 키를 지어내지 않는다.
- 같은 데이터 루트를 소유한 서버가 실행 중일 때 별도 CLI 프로세스로 루트를 열거나 SQLite를 직접 수정하지 않는다.
- **실험 실행 시에만** 시장, UTC `[start,end)`, 데이터·판단·체결 주기, warmup, 정책 revision, 초기자본, 비용, 체결/종료 규칙과 시장규칙 가정을 확인한다. 사용자가 이미 지정한 값은 재확인하지 않는다. 누락된 필수 입력(기간·시장·주기·정책·경제적 가정 등)만 묻고, 명시적으로 시나리오 제안을 요청받았다면 가정과 역사적 미검증 여부를 구분한다.
- 금액·수량·비중·bps는 스키마에 맞는 decimal 문자열이다. KST 입력은 UTC로 변환하되 기간을 임의로 늘리거나 줄이지 않는다. 예제의 날짜·수수료·호가규칙을 현재 사실이나 사용자 설정으로 자동 채택하지 않는다.

## 정책 선택 또는 작성

1. `policy_query`의 `list`로 현재 head를 찾고 `get`으로 정확한 정의를 읽는다. 실행용 참조는 반환된 `{policy_id, revision_id, definition_digest}` 전체를 보관한다.
2. 기존 정책 실행만 요청받았다면 수정하지 않는다. 수정 요청은 전체 정의를 복사해 의도한 부분만 바꾸고 `policy_write(action="revise")`에 현재 `expected_parent_revision_id`와 새 `request_id`를 넣는다. 새 정책은 `action="create"`다.
3. 부모 revision 충돌이면 읽어 둔 원본·의도한 변경·최신 head를 비교한다. 필드와 의미가 독립적임을 확인한 변경만 최신 부모와 새 request ID로 다시 적용한다. 동일 필드나 연관 조건이 충돌하면 차이를 제시하고 사용자 선택을 받는다. 기존 revision과 과거 실행은 보존한다.
4. `BUILTIN` 매개변수 변경뿐 아니라 전체 프로그램을 `RULES`로 교체할 수 있다. family는 분류용 메타데이터이므로 이름이 S1이라고 실행 로직도 S1이라고 단정하지 않는다.
5. 사용자 정의 정책은 유한 JSON DSL이다. 지원하지 않는 로직을 임의의 Python/Rust/SQL 코드로 제출하지 않는다. 순서상 첫 조건이 선택되며 상태 갱신은 모두 이전 상태를 읽는다. `HOLD`는 실제 보유 비중 유지이지, 미체결 목표의 기억이 아니다. 필요한 기억은 명시적 state로 표현한다.

등록/수정 후 반환된 revision을 다시 확인한다. 새 실험은 schema `2.0`,
`strategies: []`, `policy_selections: [정확한 참조들]`을 사용한다. 기존 v1 실행은 변환하지 않는다.

## 데이터 → 계획 → 실행

- 먼저 `dataset_query`로 요청 범위·주기·warmup에 맞는 기존 스냅샷을 찾는다. 적합하지 않으면 `collect_data`에 명시적 `CollectRequest`를 제출한다. 수집 범위와 warmup을 합친 크기도 한도에 포함된다.
- 수집 작업의 terminal output에서 `dataset_id`를 얻어 coverage·quality를 확인한다. 점검 공지와 일치해도 `UNKNOWN` 또는 `BLOCKED_DATA`가 자동 해제되지는 않는다. 가격 보간, 시간 삭제, 가짜 봉으로 통과시키지 않는다.
- 여러 dataset을 한 계획에 넣을 때는 warmup을 포함한 관측 구간 중복을 피한다. 파생 dataset에는 원본 dataset closure도 포함한다.
- 전체 `PlanRequest`를 `plan_backtest`에 제출한다. v1 예제를 v2로 쓸 때 `strategies`를 비우고 revision 참조를 넣되 나머지 필수 가정도 모두 채운다.
- 계획의 `admissions`, `warnings`, `active_limits`, `request_size`를 읽는다. 한도 거절의 requested/allowed/excess와 줄일 조건을 전달한다. 분할·기간 축소·종목/정책 제외는 사용자 지시나 이미 합의한 범위에 따라 명시적으로 정하고 새 요청으로 남긴다.
- 기간을 여러 run으로 나누면 각 run이 현금과 초기 정책 상태에서 다시 시작하므로, 단일 연속 실험과 동등하다고 보고하지 않는다.
- Evidence가 없는 S5/coverage control의 차단은 수익률 0이 아니다. Evidence를 꾸며 넣지 않는다. 사용자가 요청한 실험에 차단 모델이 있어도 임의로 제외하지 않고 계획에 표시한다.
- 실행할 계획에서 반환된 `id`와 `input_digest`를 그대로 사용한다:

```text
run_backtests({request_id: 실행 요청 ID, plan_id: 계획.id, input_digest: 계획.input_digest})
job_control({action: "get", job_id: 반환된 job.id})
```

## 작업과 재시도

- 제출 응답은 완료 증거가 아니다. `job_control(get)`으로 해당 attempt의 terminal 상태와 output을 확인한다. `PARTIAL`, `BLOCKED`, `FAILED`, `CANCELLED`, `INTERRUPTED`를 `COMPLETED`와 구분한다.
- polling은 연속 호출 대신 간격과 작업에 맞는 유한 관찰 시간을 둔다. 관찰 시간이 끝나면 job ID와 현재 상태를 남긴다. 사용자가 중단을 요청하지 않았다면 관찰 만료만으로 취소하지 않는다.
- `request_id`는 동일한 정규화 요청의 멱등성 키다. 내용을 바꾸면 새 ID를 사용한다. timeout/`OUTCOME_UNKNOWN`이면 기존 job 또는 history에서 durable 상태를 먼저 조회한다. 새 ID로 같은 작업을 중복 제출하지 않는다.
- `job_control(retry)`는 새 attempt를 만든다. 원인과 기존 상태를 확인한 뒤 사용자 요청 범위에서만 유한하게 재시도한다. `cancel`은 취소 요청일 뿐, terminal 확인 전 완료라고 말하지 않는다.
- 진행 건수는 `count_unit`과 함께 읽고 `null`을 0으로 바꾸지 않는다. 서버의 파일/큐/이벤트 제한을 우회하지 않는다.

완료 보고는 수행한 단계에 맞춘다. 정책 작업이면 revision, 수집이면 dataset/job ID,
실험이면 run/job ID·revision·input digest·가정·완료/차단 모델 수를 남긴다.
실패/미확인 범위를 구분하고 수익성이 검증됐다는 결론을 만들지 않는다.

## 필요한 자료만 읽기

아래 상대 경로는 이 저장소 안에서 제공된다. 다른 환경으로 스킬만 옮겼다면 실제 MCP
스키마를 확인하고, 필요한 문서/도구가 없으면 그 한계를 알린다.

- 요청 작성: [운영 가이드](../../docs/operator-guide.md), [plan-request 스키마](../../docs/schema/plan-request.json), [수집 예제](../../docs/examples/collect-h1.json), [전체 실험 예제](../../docs/examples/plan-h1.json).
- 사용자 정책: [정의 스키마](../../docs/schema/policy-definition.json), [EMA 교차 예제](../../docs/examples/policy-create-custom.json). 예제는 전략 추천이 아니다.
- 실행 후 비교·파일 수신이 필요하면 [backcraft-results](../backcraft-results/SKILL.md)를 읽는다.
