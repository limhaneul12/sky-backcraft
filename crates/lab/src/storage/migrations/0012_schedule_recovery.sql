CREATE TABLE schedule_recoveries (
    schedule_id TEXT PRIMARY KEY REFERENCES collection_schedules(id) ON DELETE CASCADE,
    failure_class TEXT CHECK (failure_class IN ('recoverable', 'operator_required')),
    state TEXT NOT NULL CHECK (state IN (
        'active', 'degraded', 'recovery_wait', 'probing', 'backfilling',
        'verifying_freshness', 'operator_required'
    )),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_probe_at_ms INTEGER,
    last_recovery_at_ms INTEGER,
    gap_start_ms INTEGER,
    gap_end_ms INTEGER,
    next_chunk_start_ms INTEGER,
    backfill_job_id TEXT REFERENCES jobs(id),
    backfill_attempt_id TEXT,
    backfill_generation INTEGER NOT NULL DEFAULT 0 CHECK (backfill_generation >= 0),
    next_recovery_at_ms INTEGER,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (backfill_attempt_id, backfill_job_id)
        REFERENCES job_attempts(id, job_id),
    CHECK (
        (gap_start_ms IS NULL AND gap_end_ms IS NULL AND next_chunk_start_ms IS NULL)
        OR (gap_start_ms IS NOT NULL AND gap_end_ms IS NOT NULL
            AND next_chunk_start_ms IS NOT NULL
            AND gap_start_ms < gap_end_ms
            AND next_chunk_start_ms >= gap_start_ms
            AND next_chunk_start_ms <= gap_end_ms)
    ),
    CHECK (
        (backfill_job_id IS NULL AND backfill_attempt_id IS NULL)
        OR (backfill_job_id IS NOT NULL AND backfill_attempt_id IS NOT NULL)
    )
) STRICT;

CREATE INDEX schedule_recoveries_due
ON schedule_recoveries(state, next_recovery_at_ms, schedule_id);

CREATE TABLE schedule_recovery_chunks (
    schedule_id TEXT NOT NULL REFERENCES collection_schedules(id) ON DELETE CASCADE,
    target_ms INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    generation INTEGER NOT NULL CHECK (generation >= 0),
    range_start_ms INTEGER NOT NULL,
    range_end_ms INTEGER NOT NULL CHECK (range_end_ms > range_start_ms),
    request_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN (
        'queued', 'running', 'completed', 'failed', 'cancelled'
    )),
    job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
    attempt_id TEXT NOT NULL UNIQUE,
    dataset_id TEXT REFERENCES datasets(id),
    failure_json TEXT,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (schedule_id, target_ms, chunk_index, generation),
    FOREIGN KEY (attempt_id, job_id) REFERENCES job_attempts(id, job_id)
) STRICT;

CREATE INDEX schedule_recovery_chunks_progress
ON schedule_recovery_chunks(schedule_id, target_ms, status, chunk_index, generation);

INSERT INTO schedule_recoveries(
    schedule_id, failure_class, state, attempt_count, next_recovery_at_ms, updated_at_ms
)
SELECT id,
       CASE
         WHEN status='blocked' AND json_extract(failure_json, '$.code') IN (
              'NETWORK_UNAVAILABLE', 'RATE_LIMITED', 'TEMPORARILY_BLOCKED'
         ) THEN 'recoverable'
         WHEN status='blocked' THEN 'operator_required'
         ELSE NULL
       END,
       CASE
         WHEN status='blocked' AND json_extract(failure_json, '$.code') IN (
              'NETWORK_UNAVAILABLE', 'RATE_LIMITED', 'TEMPORARILY_BLOCKED'
         ) THEN 'recovery_wait'
         WHEN status='blocked' THEN 'operator_required'
         ELSE 'active'
       END,
       0,
       CASE WHEN status='blocked' THEN next_action_at_ms ELSE NULL END,
       created_at_ms
FROM collection_schedules;

UPDATE collection_schedules
SET status='active'
WHERE id IN (
    SELECT schedule_id FROM schedule_recoveries WHERE state='recovery_wait'
);
