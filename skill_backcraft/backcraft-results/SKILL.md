---
name: backcraft-results
description: Sky Backcraft의 기존 실행과 이력을 조회하고 종목·정책별 성과, 비용 단위, 청산 이유를 분석하며 전체 export 파일을 수신·검산한다. 결과 비교, JSON 추출, 파일 다운로드, 검증 요청에 사용한다. 새 데이터 수집·정책 변경·백테스트 실행은 backcraft-experiment의 범위다.
---

# Backcraft 결과 분석·파일 수신

기존 실행을 정확히 설명하고, 요청받은 파일은 **클라이언트에 실제 수신한 뒤** 전달한다.
결과 분석만 요청받았다면 새 백테스트나 정책 수정을 시작하지 않는다.

## 실행 찾기와 비교

- 현재 MCP 도구/스키마를 확인한다. 주어진 `run_id`가 있으면 바로 조회한다. 없으면 `history_query(action="runs")`에서 찾고 후보가 여럿이면 날짜·시장·정책으로 좁힌다. 표시 이름이나 과거 예제 ID로 추정하지 않는다.
- 먼저 `result_query({action:"summary", run_id})` 한 번으로 모든 모델을 비교한다. `market`, `strategy`, `policy_ref`, `status`, `initial_cash`, `terminal_equity`, `total_return_ratio`, `total_fees`, `fill_count`, `closed_positions`, `open_positions`, 사유를 사용한다.
- family가 같아도 다른 revision/model은 별개다. 각 모델은 독립 자본 계정이므로 비교 전략들의 기말자산을 합쳐 하나의 포트폴리오 수익률로 보고하지 않는다.
- `total_return_ratio`는 비율이다. 퍼센트 표시는 ×100하고 원본 JSON 값과 단위를 보존한다. `null`과 `financial_null_reason`은 0/무손실로 바꾸지 않는다. 반대로 실제 완료됐지만 매매가 없었던 모델의 0 수익률은 차단 모델과 구분한다.
- 상세 확인이 필요할 때만 `result_query(action="costs", run_id, model_id)` 또는 `action="ledger"`를 사용한다. ledger 인자는 `query` 안에 넣는다:

```text
{action:"ledger", query:{run_id, model_id, section:"episodes", range:null, cursor:null, limit:100}}
```

`signals`, `orders`, `order_events`, `fills`, `episodes`, `equity` 중 필요한 section만
읽고 반환된 cursor를 그대로 이어간다. 한 페이지나 잘린 출력으로 전체 건수를 추정하지 않는다.
전체 원장이 필요하면 페이지 조회로 재구성하지 말고 export 파일을 수신한다.

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
python3 scripts/download-export.py \
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

## 검산, 이력, 보고

- 독립 검산 요청은 `verify_run({request_id,artifact_id,replay:false})`로 제출하고 완료된 report의 status/findings를 확인한다. 재생 요청에만 `replay:true`를 사용한다. artifact ID는 해당 패키지에서 실제 반환된 것을 쓴다.
- 로컬 파일은 `spot-lab verify-export --directory <수신 디렉터리>`로 검산한다. `replay-export`는 기록된 source/lock/toolchain/engine 조건을 충족해야 한다. 불일치를 숨기거나 버전 검사를 끄지 않는다. 원래 소스의 재생과 현재 소스의 독립 검산을 구분한다.
- 기존 파일·원장·run 해시는 보존한다. 구버전 숫자를 원본 파일에 덮어써 고치지 않는다. 비교 보고에는 단위·schema와 수정 전후 의미를 함께 적는다.
- `cause=UNKNOWN`에 `OFFICIAL_COMPLETED_MAINTENANCE_MATCH`가 붙어도 실제 중단 시작이 확인된 것은 아니다. 근거와 불확실성을 함께 표시하고, 봉을 보간하거나 차단을 풀지 않는다.
- `lab_status`의 정적 구현 라벨, CI 성공, 파일 해시 일치, 독립 검산, 재생 성공은 서로 다른 증거다. 실제 확인한 단계만 성공으로 보고한다.

최종 답변에는 비교 요약, run/정책 revision 식별자, null/차단 사유, 수행한 검증,
실제로 저장된 파일 링크와 해시를 필요한 만큼 제공한다. 파일이 없으면 가짜 링크를 만들지 않는다.

## 필요한 자료만 읽기

저장소 밖에서는 상대 경로 대신 실제 MCP 스키마를 확인한다.

- 필드 해석: [데이터 사전](../../docs/data-dictionary.md), [result-query 스키마](../../docs/schema/mcp-result-query.json).
- 수신·작업: [운영 가이드](../../docs/operator-guide.md), [수신기](../../scripts/download-export.py), [artifact 응답 스키마](../../docs/schema/artifact-query-result.json).
- 점검 근거: [공백 분류 기준](../../docs/upbit-maintenance.md).
