CREATE TABLE oauth_public_clients (
    registration_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer TEXT NOT NULL,
    client_id TEXT NOT NULL CHECK(length(client_id) BETWEEN 1 AND 256),
    redirect_uris_json TEXT NOT NULL,
    client_name TEXT NOT NULL CHECK(length(client_name) BETWEEN 1 AND 120),
    registered_at_utc TEXT NOT NULL,
    UNIQUE(issuer, client_id)
) STRICT;
CREATE INDEX oauth_public_clients_by_issuer
ON oauth_public_clients(issuer, registration_sequence);
