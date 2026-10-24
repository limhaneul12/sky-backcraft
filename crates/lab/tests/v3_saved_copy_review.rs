//! Opt-in acceptance on a fenced backup, never on the original runtime root.

use rusqlite::OpenFlags;
use spot_lab::contracts::{
    ContentHash, PaperReviewSelection, PaperSessionId, PolicyDefinition, PolicyRevisionRef,
};
use spot_lab::storage::Store;
use std::io::Write;
use std::os::unix::fs::MetadataExt;

#[test]
#[ignore = "requires an explicitly fenced saved-review DB backup"]
#[expect(
    clippy::too_many_lines,
    reason = "one fenced backup journey checks historical identities and unchanged saved rows"
)]
fn saved_copy_policy_identity_and_passive_audits() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::PathBuf::from(std::env::var("BACKCRAFT_SAVED_REVIEW_COPY_ROOT")?)
        .canonicalize()?;
    let authorized_root = std::path::Path::new(
        "/Volumes/256.SSD/RustBuild/sky-backcraft/v3-saved-review-copy-20261020",
    )
    .canonicalize()?;
    assert_eq!(
        root, authorized_root,
        "only the task-owned backup is permitted"
    );
    let original =
        std::path::Path::new("/Users/imhaneul/Library/Application Support/Sky Backcraft/research")
            .canonicalize()?;
    assert!(
        !root.starts_with(original),
        "original runtime roots are prohibited"
    );
    let db = root.join("lab.sqlite");
    assert_eq!(db.canonicalize()?, db, "input DB cannot be symlinked");
    assert_eq!(
        std::fs::metadata(&db)?.nlink(),
        1,
        "input DB cannot be hardlinked"
    );
    assert!(
        root.join("RAW_COPY_VERIFIED").is_file(),
        "raw backup receipt required"
    );
    assert_eq!(
        root.join("raw").canonicalize()?,
        root.join("raw"),
        "raw root cannot be symlinked"
    );
    assert!(
        root.join("SAVED_REVIEW_COPY_ONLY").is_file(),
        "backup marker required"
    );
    assert!(
        !root.to_string_lossy().contains("Application Support"),
        "original runtime roots are prohibited"
    );
    let connection = rusqlite::Connection::open_with_flags(
        root.join("lab.sqlite"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let before = saved_rows(&connection)?;
    let store = Store::open(&root)?;
    let mut statement = connection.prepare("SELECT policy_id,revision_id,definition_digest,definition_json FROM policy_revisions ORDER BY policy_id,revision_id")?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let mut policy_count = 0;
    for row in rows {
        let (policy_id, revision_id, digest, raw) = row?;
        let reference: PolicyRevisionRef = serde_json::from_value(
            serde_json::json!({"policy_id":policy_id,"revision_id":revision_id,"definition_digest":digest}),
        )?;
        let definition: PolicyDefinition = serde_json::from_str(&raw)?;
        assert_eq!(
            ContentHash::of_value(&definition)?,
            reference.definition_digest
        );
        assert!(store.load_policy_revision(&reference)?.is_some());
        policy_count += 1;
    }
    assert_eq!(
        policy_count, 26,
        "all baseline policy revisions must be present"
    );
    let session = PaperSessionId::new(
        "paper-session-83af0a9b150da971ee2a4616b8e821c5e981f6b88d7d756469f7ccc5f57bc555",
    )?;
    let session_projection =
        store.paper_get_session(&session, spot_lab::contracts::UtcTimestamp::now())?;
    assert!(
        session_projection
            .session
            .markets
            .iter()
            .all(|market| market.verification.status
                != spot_lab::contracts::PaperVerificationStatus::Fail)
    );
    let mut reviews = Vec::new();
    for event_sequence in [1108, 1109, 1110] {
        let review = store.paper_review_decision(&PaperReviewSelection {
            session_id: session.clone(),
            event_sequence,
            as_of_sequence: 1110,
            signal_id: None,
        })?;
        assert_eq!(review.event.sequence, event_sequence);
        assert!(review.data_quality.realtime_tradable.is_none());
        let rule_report = store
            .paper_rule_audit_input(&review)?
            .map(|input| spot_lab::reporting::review_audit::audit_snapshot_rules(&input))
            .transpose()?;
        let source_report = store
            .paper_indicator_audit_input(&review, 4096)?
            .map(|input| spot_lab::reporting::review_audit::audit_source_indicators(&input))
            .transpose()?;
        if let Some(report) = &rule_report {
            report.validate_digest()?;
        }
        if let Some(report) = &source_report {
            report.validate_digest()?;
        }
        reviews.push(serde_json::json!({"review":review,"rule_report":rule_report,"source_report":source_report}));
    }
    assert_eq!(before, saved_rows(&connection)?);
    let output = root.join("saved-review-results.json");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    file.write_all(&serde_json::to_vec_pretty(&serde_json::json!({"policy_count":policy_count,"saved_rows_unchanged":true,"session_projection":session_projection,"results":reviews}))?)?;
    store.close()?;
    Ok(())
}

fn saved_rows(
    connection: &rusqlite::Connection,
) -> Result<ContentHash, Box<dyn std::error::Error>> {
    let mut tables = Vec::new();
    for table in [
        "policies",
        "policy_revisions",
        "paper_sessions",
        "paper_market_checkpoints",
        "paper_session_events",
    ] {
        let mut statement = connection.prepare(&format!("SELECT * FROM {table} ORDER BY 1,2"))?;
        let columns = statement.column_count();
        let mut cursor = statement.query([])?;
        let mut rows = Vec::new();
        while let Some(row) = cursor.next()? {
            let mut cells = Vec::new();
            for column in 0..columns {
                use rusqlite::types::ValueRef;
                cells.push(match row.get_ref(column)? {
                    ValueRef::Null => serde_json::Value::Null,
                    ValueRef::Integer(value) => serde_json::json!(value),
                    ValueRef::Real(value) => serde_json::json!(value),
                    ValueRef::Text(value) => serde_json::json!(std::str::from_utf8(value)?),
                    ValueRef::Blob(value) => serde_json::json!(value),
                });
            }
            rows.push(cells);
        }
        tables.push((table, rows));
    }
    Ok(ContentHash::of_value(&tables)?)
}
