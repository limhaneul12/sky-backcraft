---
name: backcraft-maintenance
description: Sky Backcraft의 DB·WAL·파일 용량을 점검하고 요청된 백업·checkpoint·compact 또는 preview 범위가 승인된 단건·일괄 삭제를 수행한다. 저장 공간·RESOURCE_LIMIT·리소스 정리에 사용한다. 데이터 수집·실험 실행과 자동 정기 삭제는 시작하지 않는다.
---

# Backcraft 저장 점검·리소스 삭제

점검만 요청받았으면 점검·제안까지 수행한다. 백업·checkpoint·compact는 요청 범위에서
실행하며, 삭제는 **되돌릴 수 없는 하드 삭제**다. 실제 preview 범위에 대한 사용자
승인이 있을 때만 단건 또는 일괄 삭제한다. 이미 같은 범위를 명시적으로 승인했다면
재확인하지 않는다. 승인된 대상을 새 cascade 범위로 확대하지 않는다.

## 접속과 저장 상태 확인

- 실제 MCP action/schema를 확인하고 `lab_status`와 `storage_maintenance(action="usage")`를 읽는다. 도구 요약에 오래된 설명이 있어도 현재 typed action이 기준이다. `sqlite`에는
  `allocated/live/reusable/wal/limit_bytes`가 구분되어 있다.
- `reusable_bytes`는 다음 쓰기에 재사용되지만 admission 용량에서 사라지는 것은 아니다. usage의 `main_db_utilization`, `effective_db_bytes=allocated_bytes+wal_bytes`, `effective_utilization`과 raw/sealed ledger/exports/backup/전체 data root 한도를 함께 본다. `live_bytes`만 작다고 쓰기를 허용할 수 있다고 판단하지 않는다.
- 같은 데이터 루트를 실행 중인 서버가 소유하면 MCP를 사용한다. 별도 CLI로 루트를 열거나 SQLite/raw 파일을 직접 수정·삭제하지 않는다.
- 어느 경로든 `RESOURCE_LIMIT` 실패가 오면 응답에 limit 이름·허용량·사용량·단계·
  조치(remedy)가 포함되어 있다(RESOURCE_LIMIT와 INVALID_CONFIG는 원문이 전달된다).
  remedy를 그대로 따른다. remedy 없이 재시도를 반복하지 않는다.

## 백업·WAL·물리 축소

- 백업 요청은 `storage_maintenance({action:"create_backup",request_id})`, 조회는 `list_backups`를 사용한다. 서버가 관리하는 receipt의 id/bytes/manifest_digest/source_identity_digest를 확인한다. 백업 생성은 클라이언트 다운로드나 restore 검증 완료와 다르다. arbitrary filesystem path를 제출하지 않는다.
- WAL 정리가 요청됐으면 `{action:"checkpoint",mode:"PASSIVE"|"RESTART"|"TRUNCATE"}`를 사용한다. `busy`, `busy_reason`, log_frames/checkpointed_frames와 before/after를 읽는다. TRUNCATE 후 WAL0 또는 SQLITE_BUSY를 그대로 보고하며 reader를 강제 종료하거나 빠르게 반복 호출하지 않는다.
- 파일 자체 축소 요청은 `{action:"compact"}`를 사용한다. 서버가 checkpoint→VACUUM→final checkpoint→integrity check를 수행한다. 반환 integrity가 ok인지와 reclaimed_bytes를 확인한다. 삭제마다 compact하지 않는다.
- checkpoint/compact/batch delete는 실행 중 compute/collection이 있으면 거절될 수 있다. 용량 점검을 위해 작업을 임의 취소하지 않는다. 작업 상태를 확인하고 허용되는 시점에 유한하게 재시도하거나 대기 사유를 보고한다.

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
- `storage_maintenance(action="retention_candidates",cutoff,keep_recent,offset,limit)`는 preview가 포함된 제안이다. cutoff/보관 기준은 사용자 요청을 따른다. 후보가 나왔다는 이유로 삭제 승인으로 간주하지 않는다.
- 활성(queued/running) job은 cascade를 지정해도 거부된다. 중단까지 요청 범위에 포함된 경우만 cancel하고 terminal을 확인한다. scheduler-owned recovery job은 직접 cancel/retry/delete로 우회하지 않고 schedule pause/get 경로와 보호 참조를 따른다.

## Preview → 승인 → 실행

1. `resource_delete_preview({kind, ...id})`를 호출한다. 반환값에는 삭제 대상,
   cascade 그룹, 차단 참조(blockers), 유지되는 공유 자원, 회수 가능 파일 bytes,
   `expires_at`(600초)이 담겨 있다.
2. cascade 그룹과 blockers를 **사용자에게 그대로 표시**한다. 특히 dataset·policy
   삭제 시 함께 사라지는 run 목록을 숨기지 않는다.
3. 구체적인 preview 대상·cascade 범위에 대한 사용자 승인이 있는지 확인한다. 후보 제시만으로 승인으로 간주하지 않는다. 일반 정리 요청에 정확한 삭제 범위가 없다면 preview를 먼저 준비해 범위 확정을 묻는다.
4. `resource_hard_delete({preview: <응답 전체>, cascade: <preview에 따라>})`를
   호출한다. preview의 `scope_digest`·`expires_at`·`confirmation_token`을 임의로
   수정하지 않는다. 범위가 바뀌었거나 만료되면 서버가 거절하므로 재발급한다.
5. 결과의 `deleted_db_rows`/`deleted_files`/`deleted_file_bytes`를
   보고하고, 남겨진 자원이 `result_query`·`dataset_query`로 여전히 접근 가능한지
   확인한다.

승인된 여러 대상은 `{action:"hard_delete_batch",requests:[{preview,cascade},...]}`로
처리할 수 있다. 현재 한도는 1~8건이며 각 요청의 전체 preview와 개별 cascade 승인이
필요하다. 범위가 겹치면 앞선 삭제가 뒤 preview를 stale하게 만들 수 있으므로 그대로
묶지 않는다. batch는 순차 처리하고 실패 시 중단하며 **앞선 삭제까지 rollback되는
원자적 transaction이 아니다**. 성공 batch의 최종 maintenance는 한 번의 checkpoint다.

오류/응답 손실이면 batch를 재전송하지 않는다. 원본 preview의 정확한 resource kind/id와
cascade 목록을 보관하고 해당 자원의 실제 읽기 도구로 존재 여부를 확인한다. 예를 들어
dataset_query(get), job_control(get), collection_schedule(get), portfolio_backtest(get)
또는 정책 목록의 완전한 pagination을 사용한다. 한 페이지에 없거나 조회 자체가 실패한
것을 삭제 완료로 보지 않는다. 현재 생존/명확한 미존재/조회 불가를 구분해 보고한다.
삭제 결과를 복원하는 공개 durable history는 없으므로 잃어버린 DeleteOutcome의 정확한
rows/files/bytes는 복원·추정하지 않는다. 상태 확인만 요청받았으면 여기서 멈춘다.
남은 대상의 재시도도 요청 범위에 있을 때만 새 preview로 변경된 범위·만료·승인을
확인하고 진행한다. 부분 삭제를 모두 성공 또는 모두 rollback으로 보고하지 않는다.

## 경계와 주의

- 삭제 후 DB 공간은 freelist로 돌아가 다음 쓰기에 재사용된다. 기존 `vacuumed` 필드는 checkpoint 의미이므로 physical VACUUM이나 실제 파일 축소 완료로 보고하지 않는다. 축소 증거는 명시적 compact 결과에서 읽는다.
- 삭제한 builtin 정책(S1~S5 등)은 재시작해도 자동 복원되지 않는다. 복원은
  운영자의 `spot-lab reseed-policies` 실행이 필요하다고 사용자에게 알린다.
- 정책 전용 인벤토리·삭제 미리보기·감사 이력과 승인 삭제는 `policy_delete_query`(`inventory`/`preview`/`audit`) 및 `policy_delete_write`(`delete`) 계약을 사용할 수 있으며, 활성 세션이 참조 중인 정책(ACTIVE_REFERENCED)은 차단되고 과거 실행 참조 정책(HISTORICAL_ONLY)은 명시적 확인 문구·cascade 승인 후에만 삭제한다.
- preview 만료(600초) 후에는 같은 대상이라도 재발급해서 실행한다. 만료된 토큰으로
  재시도하지 않는다.
- 승인된 bounded batch는 지원한다. 자동 정기 삭제나 TTL 정책을 임의로 만들지 않는다.
- 실제 연구 자료(사용자가 만든 run·정책·데이터셋)는 사용자가 가치를 판단한다.
  "오래됐다"는 이유로 삭제 대상에 올리지 않는다.

완료 보고에는 점검한 storage 수치, 삭제한 대상과 남긴 대상, 회수한 rows/files/
bytes, 삭제 후 접근 확인 결과를 구분해 적는다. 실행하지 않은 제안은 제안으로
남긴다.

## 작업 입력의 삭제 보호

현재 배포 서버에서 Queued/Running 작업이 사용하는 dataset·plan·raw·policy·evidence와
연결된 run/export 자원은 직접·cascade·batch 삭제의 보호 대상이다. Running 작업의
cancel 요청만으로 보호가 풀리지 않으며 worker의 마지막 저장소 사용 종료까지 유지된다.
preview 이후 새 소비 작업이 생기면 실행 때 다시 검사하므로 이전 preview로 우회하지 않는다.

삭제 차단을 풀려고 원본 파일을 지우거나 작업 상태를 직접 바꾸지 않는다. 작업 취소는
사용자가 요청한 범위에서만 수행하고 실제 종료·새 preview를 확인한다. 제출·재시도는
입력 존재와 파일 검증을 통과해야 하며, 사라진 입력의 실패를 성공/빈 결과로 해석하지 않는다.
입력 보호 구현은 [판단 기록·입력 보호 계약](../../docs/recorded-decisions-and-input-lifetime-2026-10-24.md)을 따르며,
새 스킬만으로 구형 운영 서버의 보호 기능이 배포됐다고 주장하지 않는다.

## 필요한 자료만 읽기

아래 상대 경로는 이 저장소 안에서 제공된다. 다른 환경으로 스킬만 옮겼다면 실제
MCP 스키마를 확인하고, 필요한 문서/도구가 없으면 그 한계를 알린다.

- 저장·삭제 의미: [데이터 사전](../../docs/data-dictionary.md)의 저장 회계·하드
  삭제 절.
- 삭제 계약 스키마: [delete-resource](../../docs/schema/delete-resource.json),
  [delete-preview](../../docs/schema/delete-preview.json),
  [hard-delete-request](../../docs/schema/hard-delete-request.json).
- 관리 action/결과: [storage-maintenance-action](../../docs/schema/storage-maintenance-action.json), [storage-maintenance-result](../../docs/schema/storage-maintenance-result.json).
- 운영 관점 용량 정책: [운영 가이드](../../docs/operator-guide.md).
- 실험 실행이 필요하면 [backcraft-experiment](../backcraft-experiment/SKILL.md),
  남은 자료 분석은 [backcraft-results](../backcraft-results/SKILL.md)를 읽는다.
