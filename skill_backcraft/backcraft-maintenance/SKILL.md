---
name: backcraft-maintenance
description: Sky Backcraft의 저장 상태를 점검하고 용량 회수가 필요할 때 삭제 대상을 식별해 preview로 정확한 범위를 확인한 뒤, 사용자 승인을 받아 hard delete로 실제 삭제한다. 저장 공간 정리, RESOURCE_LIMIT 대응, 리소스 삭제 요청에 사용한다. 데이터 수집·백테스트 실행은 backcraft-experiment, 결과 분석은 backcraft-results의 범위다.
---

# Backcraft 저장 점검·리소스 삭제

삭제는 **되돌릴 수 없는 하드 삭제**다. 항상 `resource_delete_preview`로 범위를
확인한 뒤, 사용자가 해당 대상을 명시적으로 승인했을 때만 `resource_hard_delete`를
호출한다. 상태 점검만 요청받았다면 정리 대상을 제안하더라도 실행하지 않는다.

## 접속과 저장 상태 확인

- 연결된 MCP에서 `lab_status`의 `storage` 섹션을 읽는다. `sqlite`에는
  `allocated/live/reusable/wal/limit_bytes`가 구분되어 있다.
- `reusable_bytes`가 크면 다음 쓰기가 그 공간을 재사용하므로 파일 크기 증가 자체는
  당장 문제가 아니다. **`live_bytes`가 `limit_bytes`에 근접할 때가 정리 시점이다.**
- 어느 경로든 `RESOURCE_LIMIT` 실패가 오면 응답에 limit 이름·허용량·사용량·단계·
  조치(remedy)가 포함되어 있다(RESOURCE_LIMIT와 INVALID_CONFIG는 원문이 전달된다).
  remedy를 그대로 따른다. remedy 없이 재시도를 반복하지 않는다.

## 삭제 대상 탐색

- `history_query(runs/jobs)`, `dataset_query(list)`, `policy_query(list)`로
  기간·상태 기준 후보를 식별한다. `FAILED`/`INTERRUPTED` 시도, 더 이상 참조하지
  않는 실험 run, 중복 수집 dataset이 일반적인 후보다.
- cascade 방향은 구현된 실제 계약을 따른다. **run 삭제는 그 run을 발행한 job·
  attempt 이력도 함께 지우고**, **dataset 삭제는 derived dataset·이를 포함한
  plan·해당 plan의 run·발행 job까지 연쇄**한다. dataset이나 policy는 run 삭제의
  역방향으로 자동 삭제되지 않는다.
- 공유 raw 응답·관측값은 살아 있는 참조가 있으면 보존되며, preview의 retained
  항목으로 표시된다.
- 활성(queued/running) job은 cascade를 지정해도 거부된다. 먼저 cancel하고
  terminal 상태를 확인한다.

## Preview → 승인 → 실행

1. `resource_delete_preview({kind, ...id})`를 호출한다. 반환값에는 삭제 대상,
   cascade 그룹, 차단 참조(blockers), 유지되는 공유 자원, 회수 가능 파일 bytes,
   `expires_at`(600초)이 담겨 있다.
2. cascade 그룹과 blockers를 **사용자에게 그대로 표시**한다. 특히 dataset·policy
   삭제 시 함께 사라지는 run 목록을 숨기지 않는다.
3. 사용자 승인은 구체적 대상에 대해 명시적으로 받는다. 후보 목록을 보여준 것만으로
   승인으로 간주하지 않는다. "정리해", "삭제해" 같은 일반 요청은 대상 확정 질문을
   먼저 한다.
4. `resource_hard_delete({preview: <응답 전체>, cascade: <preview에 따라>})`를
   호출한다. preview의 `scope_digest`·`expires_at`·`confirmation_token`을 임의로
   수정하지 않는다. 범위가 바뀌었거나 만료되면 서버가 거절하므로 재발급한다.
5. 결과의 `deleted_db_rows`/`deleted_files`/`deleted_file_bytes`/`vacuumed`를
   보고하고, 남겨진 자원이 `result_query`·`dataset_query`로 여전히 접근 가능한지
   확인한다.

## 경계와 주의

- 삭제 후 파일 공간은 freelist로 반환되어 다음 쓰기에 재사용된다. SQLite 파일
  자체를 축소하는 VACUUM은 운영자 작업이며 이 스킬이 호출하지 않는다.
- 삭제한 builtin 정책(S1~S5 등)은 재시작해도 자동 복원되지 않는다. 복원은
  운영자의 `spot-lab reseed-policies` 실행이 필요하다고 사용자에게 알린다.
- preview 만료(600초) 후에는 같은 대상이라도 재발급해서 실행한다. 만료된 토큰으로
  재시도하지 않는다.
- 대량 일괄 삭제, 자동 정기 삭제, TTL 기반 삭제는 이 스킬의 범위가 아니다. 단건
  대상 + 명시적 CASCADE만 다룬다.
- 실제 연구 자료(사용자가 만든 run·정책·데이터셋)는 사용자가 가치를 판단한다.
  "오래됐다"는 이유로 삭제 대상에 올리지 않는다.

완료 보고에는 점검한 storage 수치, 삭제한 대상과 남긴 대상, 회수한 rows/files/
bytes, 삭제 후 접근 확인 결과를 구분해 적는다. 실행하지 않은 제안은 제안으로
남긴다.

## 필요한 자료만 읽기

아래 상대 경로는 이 저장소 안에서 제공된다. 다른 환경으로 스킬만 옮겼다면 실제
MCP 스키마를 확인하고, 필요한 문서/도구가 없으면 그 한계를 알린다.

- 저장·삭제 의미: [데이터 사전](../../docs/data-dictionary.md)의 저장 회계·하드
  삭제 절.
- 삭제 계약 스키마: [delete-resource](../../docs/schema/delete-resource.json),
  [delete-preview](../../docs/schema/delete-preview.json),
  [hard-delete-request](../../docs/schema/hard-delete-request.json).
- 운영 관점 용량 정책: [운영 가이드](../../docs/operator-guide.md).
- 실험 실행이 필요하면 [backcraft-experiment](../backcraft-experiment/SKILL.md),
  남은 자료 분석은 [backcraft-results](../backcraft-results/SKILL.md)를 읽는다.
