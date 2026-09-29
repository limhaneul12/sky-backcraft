CREATE TABLE dataset_derivations (
    derived_dataset_id TEXT PRIMARY KEY REFERENCES datasets(id),
    source_dataset_id TEXT NOT NULL REFERENCES datasets(id),
    transform_kind TEXT NOT NULL,
    transform_version TEXT NOT NULL,
    target_interval TEXT NOT NULL,
    closure_digest TEXT NOT NULL
) STRICT;

CREATE INDEX dataset_derivations_source
ON dataset_derivations(source_dataset_id, target_interval, derived_dataset_id);
