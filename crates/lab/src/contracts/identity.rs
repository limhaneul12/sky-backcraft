//! Namespaced identities and canonical digests; never filesystem paths.

use super::LabError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::io::{self, Write};

/// Lowercase SHA-256, validated at every serialized input boundary.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(try_from = "String", into = "String")]
#[schemars(with = "String")]
pub struct ContentHash(String);

impl ContentHash {
    #[must_use]
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(format!("{:x}", Sha256::digest(bytes)))
    }

    /// Hash deterministic Serde output without an intermediate JSON allocation.
    /// # Errors
    /// Returns a contract error if serialization fails.
    pub fn of_value(value: &impl Serialize) -> Result<Self, LabError> {
        let mut writer = HashWriter(Sha256::new());
        serde_json::to_writer(&mut writer, value)?;
        Ok(Self(format!("{:x}", writer.0.finalize())))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

struct HashWriter(Sha256);
impl Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TryFrom<String> for ContentHash {
    type Error = LabError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            Ok(Self(value))
        } else {
            Err(LabError::InvalidConfig("expected lowercase SHA-256".into()))
        }
    }
}
impl From<ContentHash> for String {
    fn from(value: ContentHash) -> Self {
        value.0
    }
}
impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

macro_rules! identifier {
    ($name:ident, $prefix:literal) => {
        #[derive(
            Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
        )]
        #[serde(try_from = "String", into = "String")]
        #[schemars(with = "String")]
        pub struct $name(String);
        impl $name {
            /// Validate an opaque identifier (never a path or authorization token).
            /// # Errors
            /// Rejects empty, oversized or unsafe identifier characters.
            pub fn new(value: impl Into<String>) -> Result<Self, LabError> {
                let value = value.into();
                if value.is_empty()
                    || value.len() > 96
                    || !value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                {
                    return Err(LabError::InvalidConfig(
                        concat!(
                            stringify!($name),
                            " must contain 1..96 ASCII letters, digits, hyphens or underscores"
                        )
                        .into(),
                    ));
                }
                Ok(Self(value))
            }
            #[must_use]
            pub fn from_seed(seed: &str) -> Self {
                Self(format!(
                    "{}-{}",
                    $prefix,
                    ContentHash::of_bytes(seed.as_bytes())
                ))
            }
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl TryFrom<String> for $name {
            type Error = LabError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

identifier!(RequestId, "request");
identifier!(DatasetId, "dataset");
identifier!(ObservationId, "obs");
identifier!(RawObjectId, "raw");
identifier!(RunId, "run");
identifier!(PlanId, "plan");
identifier!(ModelId, "model");
identifier!(JobId, "job");
identifier!(AttemptId, "attempt");
identifier!(SignalId, "signal");
identifier!(OrderId, "order");
identifier!(FillId, "fill");
identifier!(EpisodeId, "episode");
identifier!(EvidenceId, "evidence");
identifier!(EvidenceRevisionId, "revision");
identifier!(EvidenceSnapshotId, "evidence-snapshot");
identifier!(ArtifactId, "artifact");
identifier!(RuleSnapshotId, "rule");
identifier!(PolicyId, "policy");
identifier!(PolicyRevisionId, "policy-revision");

impl ArtifactId {
    /// Derive an immutable file identity within an explicit package/storage namespace.
    #[must_use]
    pub fn for_file(run_id: &RunId, relative_path: &str, sha256: &ContentHash) -> Self {
        Self::from_seed(&format!("{run_id}:{relative_path}:{sha256}"))
    }
}
