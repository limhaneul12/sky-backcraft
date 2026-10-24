CREATE INDEX paper_session_provider_proofs
    ON paper_session_events(session_id, sequence)
    WHERE json_type(event_json, '$.provider_recovery.proof_ids') IS NOT NULL;
