CREATE UNIQUE INDEX job_attempts_identity_owner
ON job_attempts(id, job_id);

CREATE TABLE research_suites (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    input_digest TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'paused', 'completed', 'blocked')),
    created_at_ms INTEGER NOT NULL,
    next_action_at_ms INTEGER NOT NULL,
    failure_json TEXT,
    frozen_json TEXT NOT NULL
) STRICT;

CREATE INDEX research_suites_due
ON research_suites(status, next_action_at_ms, id);

CREATE TABLE research_suite_folds (
    suite_id TEXT NOT NULL REFERENCES research_suites(id) ON DELETE CASCADE,
    fold_index INTEGER NOT NULL CHECK (fold_index >= 0),
    fold_json TEXT NOT NULL,
    PRIMARY KEY (suite_id, fold_index)
) STRICT;

CREATE TABLE research_suite_cases (
    id TEXT PRIMARY KEY,
    suite_id TEXT NOT NULL REFERENCES research_suites(id) ON DELETE CASCADE,
    case_index INTEGER NOT NULL CHECK (case_index >= 0),
    fold_index INTEGER,
    phase TEXT NOT NULL CHECK (phase IN ('batch', 'selection', 'evaluation')),
    scenario_index INTEGER NOT NULL CHECK (scenario_index >= 0),
    range_start_ms INTEGER NOT NULL,
    range_end_ms INTEGER NOT NULL CHECK (range_end_ms > range_start_ms),
    status TEXT NOT NULL CHECK (status IN (
        'planned', 'retry_pending', 'queued', 'running', 'completed', 'blocked',
        'failed', 'cancelled', 'interrupted'
    )),
    plan_id TEXT REFERENCES plans(id),
    job_id TEXT UNIQUE REFERENCES jobs(id),
    attempt_id TEXT UNIQUE,
    run_id TEXT UNIQUE REFERENCES runs(id),
    causal_input_digest TEXT,
    failure_json TEXT,
    UNIQUE (suite_id, case_index),
    FOREIGN KEY (suite_id, fold_index)
        REFERENCES research_suite_folds(suite_id, fold_index),
    FOREIGN KEY (attempt_id, job_id) REFERENCES job_attempts(id, job_id),
    CHECK (
        (job_id IS NULL AND attempt_id IS NULL)
        OR (job_id IS NOT NULL AND attempt_id IS NOT NULL)
    )
) STRICT;

CREATE INDEX research_suite_cases_progress
ON research_suite_cases(suite_id, status, case_index);

CREATE TABLE run_model_comparisons (
    run_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    comparison_json TEXT NOT NULL,
    PRIMARY KEY (run_id, model_id),
    FOREIGN KEY (run_id, model_id) REFERENCES run_models(run_id, model_id)
) STRICT;

CREATE TABLE collection_schedules (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    input_digest TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'paused', 'blocked')),
    created_at_ms INTEGER NOT NULL,
    next_action_at_ms INTEGER NOT NULL,
    request_json TEXT NOT NULL,
    last_success_dataset_id TEXT REFERENCES datasets(id),
    last_success_boundary_ms INTEGER,
    failure_json TEXT,
    CHECK (
        (last_success_dataset_id IS NULL AND last_success_boundary_ms IS NULL)
        OR (last_success_dataset_id IS NOT NULL AND last_success_boundary_ms IS NOT NULL)
    )
) STRICT;

CREATE INDEX collection_schedules_due
ON collection_schedules(status, next_action_at_ms, id);

CREATE TABLE schedule_fires (
    id TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL REFERENCES collection_schedules(id) ON DELETE CASCADE,
    boundary_ms INTEGER NOT NULL,
    request_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN (
        'planned', 'queued', 'running', 'retry_wait',
        'completed', 'blocked', 'cancelled'
    )),
    job_id TEXT UNIQUE REFERENCES jobs(id),
    attempt_id TEXT UNIQUE,
    dataset_id TEXT REFERENCES datasets(id),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count BETWEEN 0 AND 5),
    retry_at_ms INTEGER,
    failure_json TEXT,
    created_at_ms INTEGER NOT NULL,
    UNIQUE (schedule_id, boundary_ms),
    FOREIGN KEY (attempt_id, job_id) REFERENCES job_attempts(id, job_id),
    CHECK (
        (job_id IS NULL AND attempt_id IS NULL)
        OR (job_id IS NOT NULL AND attempt_id IS NOT NULL)
    ),
    CHECK (
        (status = 'retry_wait' AND retry_at_ms IS NOT NULL)
        OR (status <> 'retry_wait')
    )
) STRICT;

CREATE INDEX schedule_fires_progress
ON schedule_fires(schedule_id, status, boundary_ms);

CREATE TABLE managed_backups (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    input_digest TEXT NOT NULL,
    source_identity_digest TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    relative_path TEXT NOT NULL UNIQUE,
    bytes INTEGER NOT NULL CHECK (bytes >= 0),
    manifest_digest TEXT NOT NULL,
    receipt_json TEXT NOT NULL
) STRICT;
