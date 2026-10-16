CREATE TABLE policy_delete_requests (
    request_id TEXT PRIMARY KEY,
    request_digest TEXT NOT NULL,
    confirmation_text TEXT NOT NULL,
    requested_count INTEGER NOT NULL CHECK (requested_count BETWEEN 1 AND 100),
    state TEXT NOT NULL CHECK (state IN ('PENDING','COMPLETE','PARTIAL','INTERRUPTED')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE policy_delete_request_items (
    request_id TEXT NOT NULL REFERENCES policy_delete_requests(request_id) ON DELETE RESTRICT,
    position INTEGER NOT NULL CHECK (position >= 0),
    policy_id TEXT NOT NULL,
    preview_scope_digest TEXT,
    preview_digest TEXT NOT NULL,
    preview_expires_at_ms INTEGER,
    preview_confirmation_token TEXT,
    status TEXT NOT NULL CHECK (status IN (
        'PENDING','DELETED','BLOCKED','STALE','FAILED','INTERRUPTED_NOT_APPLIED'
    )),
    reason_code TEXT CHECK (reason_code IS NULL OR reason_code IN (
        'BUILTIN_POLICY','UNKNOWN_ORIGIN','POLICY_NOT_FOUND','REFERENCED_HISTORICAL',
        'REFERENCED_PAPER','REFERENCED_AUTOMATION','ACTIVE_JOB','OTHER_RESOURCE_SCOPE',
        'PREVIEW_MISSING','PREVIEW_INCOMPLETE','PREVIEW_EXPIRED','SCOPE_CHANGED',
        'TOKEN_MISMATCH','PROCESS_INTERRUPTED','STORAGE_FAILURE'
    )),
    remediation_code TEXT CHECK (remediation_code IS NULL OR remediation_code IN (
        'ARCHIVE_DEACTIVATE_UNAVAILABLE','BUILTIN_READ_ONLY','FRESH_PREVIEW_REQUIRED'
    )),
    deleted_revision_count INTEGER NOT NULL DEFAULT 0 CHECK (deleted_revision_count >= 0),
    retained INTEGER NOT NULL DEFAULT 1 CHECK (retained IN (0,1)),
    effect TEXT NOT NULL CHECK (effect IN ('APPLIED','NOT_APPLIED','NO_NEW_EFFECT')),
    PRIMARY KEY (request_id, policy_id),
    UNIQUE (request_id, position)
) STRICT;

CREATE INDEX policy_delete_request_items_order
ON policy_delete_request_items(request_id, policy_id);
