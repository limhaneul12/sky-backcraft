//! Upbit private API authentication, credential management, and JWT signing.
//!
//! Enforces:
//! - Secure credential loading from process environment or `.env` file.
//! - Strict secret redaction in all Debug/Display/logging output.
//! - Nonce generation from cryptographically secure random bytes.
//! - Canonical query hash generation using SHA-512 per Upbit specification.
//! - RFC 7519 JWT generation with HS256 signature (HMAC-SHA256) and unpadded `Base64URL`.
//! - Strict host allowlist verification (`https://api.upbit.com`).

use serde::Serialize;
use sha2::{Digest, Sha256, Sha512};
use std::fmt;
use std::fs::File;
use std::io::{Read, Take};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

const MAX_CREDENTIAL_FILE_BYTES: u64 = 64 * 1024;
#[cfg(target_os = "linux")]
const O_NOFOLLOW_FLAG: i32 = 0o400_000;
#[cfg(not(target_os = "linux"))]
const O_NOFOLLOW_FLAG: i32 = 0x100;

/// Fixed production Upbit API base URL.
pub const UPBIT_PRODUCTION_HOST: &str = "https://api.upbit.com";

/// Authentication errors.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Missing Upbit API credentials: {0}")]
    MissingCredentials(String),
    #[error("Disallowed target host: '{0}' (expected strictly '{1}')")]
    DisallowedHost(String, String),
    #[error("Randomness failure for nonce generation: {0}")]
    Randomness(String),
    #[error("Serialization failure: {0}")]
    Serialization(String),
    #[error("IO failure: {0}")]
    Io(String),
}

/// Upbit API credentials with secure redaction.
#[derive(Clone)]
pub struct UpbitCredentials {
    access_key: String,
    secret_key: String,
}

impl UpbitCredentials {
    /// Create credentials from raw access and secret keys.
    #[must_use]
    pub fn new(access_key: String, secret_key: String) -> Self {
        Self {
            access_key,
            secret_key,
        }
    }

    /// Access key string for authorized request payload.
    #[must_use]
    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    /// Secret key bytes for HMAC-SHA256 signature computation.
    #[must_use]
    pub fn secret_key_bytes(&self) -> &[u8] {
        self.secret_key.as_bytes()
    }

    /// Attempt to load credentials from process environment or `.env` files.
    ///
    /// Checks:
    /// 1. Process environment variables `SERVICE_UPBIT_PRIVATE_ACCESS_KEY` & `SERVICE_UPBIT_PRIVATE_SECRET_KEY`
    /// 2. Explicit file from `SERVICE_UPBIT_CREDENTIAL_FILE`
    /// 3. `.env` in current directory
    /// 4. `.env` in optional data root path
    ///
    /// Logs only the existence of identified variable names, never any secret content.
    ///
    /// # Errors
    /// Returns `AuthError::MissingCredentials` if keys cannot be found.
    pub fn load_from_env_or_file(optional_root: Option<&Path>) -> Result<Self, AuthError> {
        Self::load_from_sources(optional_root, true)
    }

    /// Load process/configured-file credentials, then only the supplied root .env.
    /// This keeps explicit MOCK fixtures isolated from the invoking working directory.
    /// # Errors
    /// Returns `AuthError::MissingCredentials` if keys cannot be found.
    pub fn load_from_env_or_root_file(optional_root: Option<&Path>) -> Result<Self, AuthError> {
        Self::load_from_sources(optional_root, false)
    }

    fn load_from_sources(
        optional_root: Option<&Path>,
        include_current_directory: bool,
    ) -> Result<Self, AuthError> {
        const KEY_ACCESS: &str = "SERVICE_UPBIT_PRIVATE_ACCESS_KEY";
        const KEY_SECRET: &str = "SERVICE_UPBIT_PRIVATE_SECRET_KEY";

        // 1. Process environment. A partial or malformed explicit pair is a
        // configuration error and must not silently become mock mode.
        let environment_access = std::env::var(KEY_ACCESS);
        let environment_secret = std::env::var(KEY_SECRET);
        match (environment_access, environment_secret) {
            (Ok(access), Ok(secret)) if !access.trim().is_empty() && !secret.trim().is_empty() => {
                tracing::info!(
                    "Upbit credentials loaded from process environment [{KEY_ACCESS}, {KEY_SECRET}]"
                );
                return Ok(Self::new(
                    access.trim().to_owned(),
                    secret.trim().to_owned(),
                ));
            }
            (Err(std::env::VarError::NotPresent), Err(std::env::VarError::NotPresent)) => {}
            _ => {
                return Err(AuthError::MissingCredentials(format!(
                    "{KEY_ACCESS} and {KEY_SECRET} must both be present and non-empty"
                )));
            }
        }

        // 2. The native parent passes only a file locator; Rust remains the sole
        // authority that reads and parses credential values.
        match std::env::var("SERVICE_UPBIT_CREDENTIAL_FILE") {
            Ok(value) if !value.trim().is_empty() => {
                let path = std::path::PathBuf::from(value);
                return load_credentials_file(&path, KEY_ACCESS, KEY_SECRET)?.ok_or_else(|| {
                    AuthError::MissingCredentials(
                        "configured Upbit credential file lacks the required key pair".into(),
                    )
                });
            }
            Ok(_) => {
                return Err(AuthError::MissingCredentials(
                    "SERVICE_UPBIT_CREDENTIAL_FILE cannot be empty".into(),
                ));
            }
            Err(std::env::VarError::NotPresent) => {}
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(AuthError::Io(
                    "SERVICE_UPBIT_CREDENTIAL_FILE is not valid UTF-8".into(),
                ));
            }
        }
        let mut candidates = Vec::new();
        if include_current_directory {
            candidates.push(Path::new(".env").to_path_buf());
        }
        if let Some(root) = optional_root {
            candidates.push(root.join(".env"));
        }

        for path in candidates {
            if let Some(credentials) = load_credentials_file(&path, KEY_ACCESS, KEY_SECRET)? {
                tracing::info!("Upbit credentials loaded from configured file source");
                return Ok(credentials);
            }
        }

        Err(AuthError::MissingCredentials(format!(
            "Missing required environment variables {KEY_ACCESS} and {KEY_SECRET}"
        )))
    }

    /// Sign an authenticated request and construct the Authorization Bearer header.
    ///
    /// # Errors
    /// Returns `AuthError` on randomness or serialization failure.
    pub fn sign_request(&self, query_string: Option<&str>) -> Result<String, AuthError> {
        let nonce = generate_nonce()?;
        let query_hash_info = query_string.and_then(|qs| {
            let trimmed = qs.trim_start_matches('?').trim();
            if trimmed.is_empty() {
                None
            } else {
                let mut hasher = Sha512::new();
                hasher.update(trimmed.as_bytes());
                let digest = hasher.finalize();
                let hex_hash = hex_encode(&digest);
                Some(hex_hash)
            }
        });

        let header = JwtHeader {
            alg: "HS256",
            typ: "JWT",
        };
        let header_json =
            serde_json::to_string(&header).map_err(|e| AuthError::Serialization(e.to_string()))?;
        let header_b64 = base64url_encode(header_json.as_bytes());

        let payload = JwtPayload {
            access_key: &self.access_key,
            nonce: &nonce,
            query_hash: query_hash_info.as_deref(),
            query_hash_alg: query_hash_info.as_ref().map(|_| "SHA512"),
        };
        let payload_json =
            serde_json::to_string(&payload).map_err(|e| AuthError::Serialization(e.to_string()))?;
        let payload_b64 = base64url_encode(payload_json.as_bytes());

        let signing_input = format!("{header_b64}.{payload_b64}");
        let signature = hmac_sha256(self.secret_key_bytes(), signing_input.as_bytes());
        let signature_b64 = base64url_encode(&signature);

        Ok(format!("Bearer {signing_input}.{signature_b64}"))
    }
}

#[must_use]
pub(crate) fn explicit_configuration_present() -> bool {
    [
        "SERVICE_UPBIT_PRIVATE_ACCESS_KEY",
        "SERVICE_UPBIT_PRIVATE_SECRET_KEY",
        "SERVICE_UPBIT_CREDENTIAL_FILE",
    ]
    .iter()
    .any(|name| std::env::var_os(name).is_some())
}

fn load_credentials_file(
    path: &Path,
    access_key_name: &str,
    secret_key_name: &str,
) -> Result<Option<UpbitCredentials>, AuthError> {
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(O_NOFOLLOW_FLAG)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(AuthError::Io(format!(
                "open credential file safely: {error}"
            )));
        }
    };
    let metadata = file
        .metadata()
        .map_err(|error| AuthError::Io(format!("inspect opened credential file: {error}")))?;
    if !metadata.is_file() {
        return Err(AuthError::Io(
            "credential file must be a regular non-symlink file".into(),
        ));
    }
    if metadata.len() > MAX_CREDENTIAL_FILE_BYTES {
        return Err(AuthError::Io(
            "credential file exceeds the 64 KiB limit".into(),
        ));
    }
    let mut bytes =
        Vec::with_capacity(usize::try_from(metadata.len()).map_err(|_| {
            AuthError::Io("credential file length exceeds platform capacity".into())
        })?);
    let mut bounded: Take<File> = file.take(MAX_CREDENTIAL_FILE_BYTES + 1);
    bounded
        .read_to_end(&mut bytes)
        .map_err(|error| AuthError::Io(format!("read credential file: {error}")))?;
    if bytes.len() as u64 > MAX_CREDENTIAL_FILE_BYTES {
        return Err(AuthError::Io(
            "credential file exceeds the 64 KiB limit".into(),
        ));
    }
    let content = String::from_utf8(bytes)
        .map_err(|error| AuthError::Io(format!("credential file is not UTF-8: {error}")))?;
    let mut found_access = None;
    let mut found_secret = None;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = trimmed.split_once('=') {
            let key = key.trim();
            let value = value.trim().trim_matches('"').trim_matches('\'');
            if key == access_key_name && !value.is_empty() {
                found_access = Some(value.to_owned());
            } else if key == secret_key_name && !value.is_empty() {
                found_secret = Some(value.to_owned());
            }
        }
    }
    Ok(match (found_access, found_secret) {
        (Some(access), Some(secret)) => Some(UpbitCredentials::new(access, secret)),
        _ => None,
    })
}

impl fmt::Debug for UpbitCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpbitCredentials")
            .field("access_key", &"[REDACTED]")
            .field("secret_key", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Display for UpbitCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UpbitCredentials([REDACTED])")
    }
}

/// Verify that a target URL strictly matches the allowed host prefix.
///
/// # Errors
/// Returns `AuthError::DisallowedHost` if the URL points outside `https://api.upbit.com`.
pub fn verify_host_allowlist(url: &str) -> Result<(), AuthError> {
    if let Some(remainder) = url.strip_prefix(UPBIT_PRODUCTION_HOST)
        && (remainder.is_empty() || remainder.starts_with('/') || remainder.starts_with('?'))
    {
        return Ok(());
    }
    Err(AuthError::DisallowedHost(
        url.to_string(),
        UPBIT_PRODUCTION_HOST.to_string(),
    ))
}

#[derive(Serialize)]
struct JwtHeader<'a> {
    alg: &'a str,
    typ: &'a str,
}

#[derive(Serialize)]
struct JwtPayload<'a> {
    access_key: &'a str,
    nonce: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_hash: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_hash_alg: Option<&'a str>,
}

/// Generate a cryptographically secure UUID-formatted nonce from `/dev/urandom`.
pub(crate) fn generate_nonce() -> Result<String, AuthError> {
    let mut bytes = [0u8; 16];
    let mut file = File::open("/dev/urandom")
        .map_err(|e| AuthError::Randomness(format!("cannot open /dev/urandom: {e}")))?;
    file.read_exact(&mut bytes)
        .map_err(|e| AuthError::Randomness(format!("cannot read /dev/urandom: {e}")))?;

    // Set UUID v4 variant and version bits
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // Version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // Variant 1

    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    ))
}

/// Encode bytes to lowercase hexadecimal string.
#[must_use]
pub fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Encode bytes using unpadded RFC 7515 `Base64URL`.
#[must_use]
pub fn base64url_encode(input: &[u8]) -> String {
    const CHARSET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity((input.len() * 4 / 3) + 4);

    let mut i = 0;
    while i + 3 <= input.len() {
        let n =
            (u32::from(input[i]) << 16) | (u32::from(input[i + 1]) << 8) | u32::from(input[i + 2]);
        out.push(CHARSET[((n >> 18) & 63) as usize] as char);
        out.push(CHARSET[((n >> 12) & 63) as usize] as char);
        out.push(CHARSET[((n >> 6) & 63) as usize] as char);
        out.push(CHARSET[(n & 63) as usize] as char);
        i += 3;
    }

    match input.len() - i {
        1 => {
            let n = u32::from(input[i]) << 16;
            out.push(CHARSET[((n >> 18) & 63) as usize] as char);
            out.push(CHARSET[((n >> 12) & 63) as usize] as char);
        }
        2 => {
            let n = (u32::from(input[i]) << 16) | (u32::from(input[i + 1]) << 8);
            out.push(CHARSET[((n >> 18) & 63) as usize] as char);
            out.push(CHARSET[((n >> 12) & 63) as usize] as char);
            out.push(CHARSET[((n >> 6) & 63) as usize] as char);
        }
        _ => {}
    }

    out
}

#[cfg(test)]
mod credential_file_tests {
    use super::*;
    use std::io::Write as _;
    use std::os::unix::fs::symlink;

    fn temporary_path(label: &str) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        Ok(std::env::temp_dir().join(format!(
            "spot-lab-upbit-{label}-{}-{nonce}",
            std::process::id()
        )))
    }

    fn write_mode(path: &Path, bytes: &[u8], mode: u32) -> Result<(), std::io::Error> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(mode)
            .open(path)?;
        file.write_all(bytes)
    }

    #[test]
    fn explicit_credential_file_is_bounded_and_parsed_by_rust()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_path("credentials")?;
        write_mode(
            &path,
            b"SERVICE_UPBIT_PRIVATE_ACCESS_KEY=access-test\nSERVICE_UPBIT_PRIVATE_SECRET_KEY=secret-test\n",
            0o600,
        )?;
        let credentials = load_credentials_file(
            &path,
            "SERVICE_UPBIT_PRIVATE_ACCESS_KEY",
            "SERVICE_UPBIT_PRIVATE_SECRET_KEY",
        )?
        .ok_or("credential file was not parsed")?;
        assert_eq!(credentials.access_key(), "access-test");
        assert_eq!(credentials.secret_key_bytes(), b"secret-test");
        assert!(!format!("{credentials:?}").contains("access-test"));
        assert!(!credentials.to_string().contains("secret-test"));
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn credential_file_accepts_0644_and_rejects_symlink_and_size()
    -> Result<(), Box<dyn std::error::Error>> {
        let malformed = temporary_path("malformed")?;
        write_mode(&malformed, b"UNRELATED=value\n", 0o600)?;
        assert!(load_credentials_file(&malformed, "ACCESS", "SECRET")?.is_none());

        let compatible = temporary_path("compatible-0644")?;
        write_mode(&compatible, b"ACCESS=a\nSECRET=b\n", 0o644)?;
        assert!(load_credentials_file(&compatible, "ACCESS", "SECRET")?.is_some());

        let oversized = temporary_path("oversized")?;
        write_mode(
            &oversized,
            &vec![b'x'; usize::try_from(MAX_CREDENTIAL_FILE_BYTES + 1)?],
            0o600,
        )?;
        assert!(load_credentials_file(&oversized, "ACCESS", "SECRET").is_err());

        let target = temporary_path("target")?;
        let link = temporary_path("symlink")?;
        write_mode(&target, b"ACCESS=a\nSECRET=b\n", 0o600)?;
        symlink(&target, &link)?;
        assert!(load_credentials_file(&link, "ACCESS", "SECRET").is_err());

        std::fs::remove_file(malformed)?;
        std::fs::remove_file(compatible)?;
        std::fs::remove_file(oversized)?;
        std::fs::remove_file(link)?;
        std::fs::remove_file(target)?;
        Ok(())
    }
}

/// Compute HMAC-SHA256 per FIPS 198-1 using `sha2::Sha256`.
#[must_use]
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        let hash = Sha256::digest(key);
        k[..32].copy_from_slice(&hash);
    } else {
        k[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0u8; 64];
    let mut opad = [0u8; 64];
    for i in 0..64 {
        ipad[i] = k[i] ^ 0x36;
        opad[i] = k[i] ^ 0x5c;
    }

    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(message);
    let inner_hash = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_hash);
    let outer_hash = outer.finalize();

    outer_hash.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secret_redaction() {
        let creds = UpbitCredentials::new("my-access-key".into(), "my-secret-key".into());
        let debug_str = format!("{creds:?}");
        assert!(!debug_str.contains("my-access-key"));
        assert!(!debug_str.contains("my-secret-key"));
        assert!(debug_str.contains("[REDACTED]"));

        let disp_str = format!("{creds}");
        assert!(!disp_str.contains("my-access-key"));
        assert!(!disp_str.contains("my-secret-key"));
        assert!(disp_str.contains("[REDACTED]"));
    }

    #[test]
    fn test_rfc4231_hmac_sha256_case1() {
        // RFC 4231 Test Case 1: key = 0x0b repeated 20 times, data = "Hi There"
        let key = [0x0bu8; 20];
        let data = b"Hi There";
        let expected_hex = "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7";
        let mac = hmac_sha256(&key, data);
        assert_eq!(hex_encode(&mac), expected_hex);
    }

    #[test]
    fn test_rfc4231_hmac_sha256_case2() {
        // RFC 4231 Test Case 2: key = "Jefe", data = "what do ya want for nothing?"
        let key = b"Jefe";
        let data = b"what do ya want for nothing?";
        let expected_hex = "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843";
        let mac = hmac_sha256(key, data);
        assert_eq!(hex_encode(&mac), expected_hex);
    }

    #[test]
    fn test_base64url_encode() {
        assert_eq!(base64url_encode(b""), "");
        assert_eq!(base64url_encode(b"f"), "Zg");
        assert_eq!(base64url_encode(b"fo"), "Zm8");
        assert_eq!(base64url_encode(b"foo"), "Zm9v");
        assert_eq!(base64url_encode(b"foob"), "Zm9vYg");
        assert_eq!(base64url_encode(b"fooba"), "Zm9vYmE");
        assert_eq!(base64url_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn test_host_allowlist_enforcement() {
        assert!(verify_host_allowlist("https://api.upbit.com").is_ok());
        assert!(verify_host_allowlist("https://api.upbit.com/v1/accounts").is_ok());
        assert!(
            verify_host_allowlist("https://api.upbit.com/v1/orders/chance?market=KRW-BTC").is_ok()
        );

        assert!(verify_host_allowlist("http://api.upbit.com/v1/accounts").is_err());
        assert!(verify_host_allowlist("https://malicious-upbit.com/v1/accounts").is_err());
        assert!(verify_host_allowlist("https://api.upbit.com.attacker.com/v1").is_err());
        assert!(verify_host_allowlist("https://google.com").is_err());
    }

    #[test]
    fn test_jwt_generation_with_and_without_query() {
        let creds = UpbitCredentials::new("test-access".into(), "test-secret".into());

        // Without query
        let token_no_query = creds.sign_request(None).unwrap();
        assert!(token_no_query.starts_with("Bearer "));
        let raw_token = &token_no_query["Bearer ".len()..];
        let parts: Vec<&str> = raw_token.split('.').collect();
        assert_eq!(parts.len(), 3);

        // With query
        let token_with_query = creds.sign_request(Some("market=KRW-BTC")).unwrap();
        let raw_token_q = &token_with_query["Bearer ".len()..];
        let parts_q: Vec<&str> = raw_token_q.split('.').collect();
        assert_eq!(parts_q.len(), 3);
        assert_ne!(parts[1], parts_q[1]); // Payloads differ because of query_hash
    }
}
