//! Durable public OAuth client metadata. Authorization grants remain process-local.

use super::{Store, json_error, sql_error};
use crate::contracts::LabError;
use rusqlite::params;

#[derive(Debug, Clone)]
pub(crate) struct NewOAuthClientRecord {
    pub issuer: String,
    pub client_id: String,
    pub redirect_uris: Vec<String>,
    pub client_name: String,
    pub registered_at_utc: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredOAuthClientRecord {
    pub registration_sequence: u64,
    pub issuer: String,
    pub client_id: String,
    pub redirect_uris: Vec<String>,
    pub client_name: String,
    pub registered_at_utc: String,
}

impl Store {
    /// Load public client identities in their stable insertion order.
    ///
    /// # Errors
    /// Fails closed on malformed persisted callback metadata or SQL errors.
    pub(crate) fn load_oauth_clients(
        &self,
        issuer: &str,
        maximum: usize,
    ) -> Result<Vec<StoredOAuthClientRecord>, LabError> {
        let query_limit = maximum
            .checked_add(1)
            .ok_or_else(|| LabError::ResourceLimit("OAuth client load limit overflow".into()))?;
        let query_limit = i64::try_from(query_limit).map_err(|_| {
            LabError::ResourceLimit("OAuth client load limit exceeds SQLite".into())
        })?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT registration_sequence, issuer, client_id, redirect_uris_json, client_name, registered_at_utc \
                 FROM oauth_public_clients WHERE issuer=?1 ORDER BY registration_sequence ASC LIMIT ?2",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![issuer, query_limit], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })
            .map_err(sql_error)?;

        let mut clients = Vec::new();
        for row in rows {
            let (sequence, issuer, client_id, redirect_uris_json, client_name, registered_at_utc) =
                row.map_err(sql_error)?;
            let registration_sequence = u64::try_from(sequence).map_err(|_| {
                LabError::DataCorrupt("OAuth client registration sequence is invalid".into())
            })?;
            if registration_sequence == 0 {
                return Err(LabError::DataCorrupt(
                    "OAuth client registration sequence is zero".into(),
                ));
            }
            let redirect_uris = serde_json::from_str(&redirect_uris_json).map_err(|_| {
                LabError::DataCorrupt("stored OAuth client callback metadata is invalid".into())
            })?;
            clients.push(StoredOAuthClientRecord {
                registration_sequence,
                issuer,
                client_id,
                redirect_uris,
                client_name,
                registered_at_utc,
            });
        }
        if clients.len() > maximum {
            return Err(LabError::DataCorrupt(
                "stored OAuth client count exceeds the configured capacity".into(),
            ));
        }
        Ok(clients)
    }

    /// Atomically evict one previously selected unused client and persist the new DCR identity.
    ///
    /// The caller reserves a selected in-memory eviction candidate before this SQL transaction,
    /// then reacquires the grants lock to publish the result after commit.
    ///
    /// # Errors
    /// Rolls back both changes on capacity, duplicate identity, or SQLite failure.
    pub(crate) fn register_oauth_client(
        &mut self,
        client: &NewOAuthClientRecord,
        evict_client_id: Option<String>,
    ) -> Result<u64, LabError> {
        let redirect_uris_json =
            serde_json::to_string(&client.redirect_uris).map_err(json_error)?;
        let transaction = self.connection.transaction().map_err(sql_error)?;
        if let Some(evict_client_id) = evict_client_id {
            let removed = transaction
                .execute(
                    "DELETE FROM oauth_public_clients WHERE issuer=?1 AND client_id=?2",
                    params![client.issuer, evict_client_id],
                )
                .map_err(sql_error)?;
            if removed != 1 {
                return Err(LabError::DataCorrupt(
                    "OAuth eviction candidate is missing from durable registry".into(),
                ));
            }
        }
        transaction
            .execute(
                "INSERT INTO oauth_public_clients(\
                    issuer, client_id, redirect_uris_json, client_name, registered_at_utc\
                 ) VALUES (?1,?2,?3,?4,?5)",
                params![
                    client.issuer,
                    client.client_id,
                    redirect_uris_json,
                    client.client_name,
                    client.registered_at_utc,
                ],
            )
            .map_err(|error| match error {
                rusqlite::Error::SqliteFailure(ref code, _)
                    if code.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    LabError::Conflict("OAuth client ID already exists in durable registry".into())
                }
                other => sql_error(other),
            })?;
        let sequence = u64::try_from(transaction.last_insert_rowid())
            .map_err(|_| LabError::ResourceLimit("OAuth client sequence overflow".into()))?;
        transaction.commit().map_err(sql_error)?;
        Ok(sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "sky-backcraft-oauth-client-{label}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock after epoch")
                    .as_nanos()
            ));
            std::fs::create_dir_all(&path).expect("create test root");
            Self(path)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn client(client_id: &str) -> NewOAuthClientRecord {
        NewOAuthClientRecord {
            issuer: "https://skybackcraft.store".into(),
            client_id: client_id.into(),
            redirect_uris: vec!["http://127.0.0.1/callback".into()],
            client_name: format!("Client {client_id}"),
            registered_at_utc: "2026-10-17T00:00:00Z".into(),
        }
    }

    #[test]
    fn client_registration_eviction_is_atomic_and_ordered() {
        let root = TestRoot::new("atomic-eviction");
        let mut store = Store::open(root.0.clone()).expect("open store and apply migrations");
        let first = store
            .register_oauth_client(&client("client-a"), None)
            .expect("persist first client");
        let second = store
            .register_oauth_client(&client("client-b"), None)
            .expect("persist second client");
        assert!(second > first);

        assert!(
            store
                .register_oauth_client(&client("client-b"), Some("client-a".into()))
                .is_err(),
            "a duplicate identity must fail"
        );
        let after_failed_write = store
            .load_oauth_clients("https://skybackcraft.store", 64)
            .expect("read after rollback");
        assert_eq!(
            after_failed_write
                .iter()
                .map(|item| item.client_id.as_str())
                .collect::<Vec<_>>(),
            ["client-a", "client-b"]
        );

        let third = store
            .register_oauth_client(&client("client-c"), Some("client-a".into()))
            .expect("atomically evict and insert");
        assert!(third > second);
        let clients = store
            .load_oauth_clients("https://skybackcraft.store", 64)
            .expect("read durable clients");
        assert_eq!(
            clients
                .iter()
                .map(|item| item.client_id.as_str())
                .collect::<Vec<_>>(),
            ["client-b", "client-c"]
        );
        assert_eq!(clients[0].registration_sequence, second);
        assert_eq!(clients[1].registration_sequence, third);
    }

    #[test]
    fn malformed_persisted_callback_json_fails_closed() {
        let root = TestRoot::new("corrupt-metadata");
        let mut store = Store::open(root.0.clone()).expect("open store and apply migrations");
        store
            .register_oauth_client(&client("client-a"), None)
            .expect("persist client");
        store
            .connection
            .execute(
                "UPDATE oauth_public_clients SET redirect_uris_json='not-json' WHERE client_id='client-a'",
                [],
            )
            .expect("corrupt only callback metadata");
        assert!(matches!(
            store.load_oauth_clients("https://skybackcraft.store", 64),
            Err(LabError::DataCorrupt(_))
        ));
    }

    #[test]
    fn public_client_ids_are_isolated_by_exact_issuer() {
        let root = TestRoot::new("issuer-scope");
        let mut store = Store::open(root.0.clone()).expect("open store and apply migrations");
        store
            .register_oauth_client(&client("shared-public-id"), None)
            .expect("persist first issuer client");
        store
            .register_oauth_client(
                &NewOAuthClientRecord {
                    issuer: "https://other.example".into(),
                    client_id: "shared-public-id".into(),
                    redirect_uris: vec!["https://other.example/callback".into()],
                    client_name: "Other issuer client".into(),
                    registered_at_utc: "2026-10-17T00:00:01Z".into(),
                },
                None,
            )
            .expect("same public ID under a different issuer");

        let backcraft = store
            .load_oauth_clients("https://skybackcraft.store", 64)
            .expect("read first issuer clients");
        let other = store
            .load_oauth_clients("https://other.example", 64)
            .expect("read other issuer clients");
        assert_eq!(backcraft.len(), 1);
        assert_eq!(backcraft[0].client_name, "Client shared-public-id");
        assert_eq!(other.len(), 1);
        assert_eq!(other[0].client_name, "Other issuer client");
    }
}
