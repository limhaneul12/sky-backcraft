-- Durable minimal progress record for hard deletions with exclusive files.
-- Rows exist only while committed DB deletions still have pending file removals;
-- recovery removes the listed files and clears the row. No deleted business
-- content is preserved here: paths and sizes only.

CREATE TABLE deletion_journal (
    id INTEGER PRIMARY KEY,
    resource_kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    scope_digest TEXT NOT NULL,
    pending_files_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
) STRICT;
