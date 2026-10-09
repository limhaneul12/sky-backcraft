//! Fail-closed validation for build verification receipts.

use serde::{Deserialize, Serialize};

const RECEIPT_VERSION: u32 = 1;
const MAX_RECEIPT_BYTES: usize = 8_192;
const CANONICAL_CI_COMMAND: &str = "cargo xtask ci";
const PUBLIC_SCHEMA_COMMAND: &str = "spot-lab schemas + TCP tools/list consistency";
const CRITICAL_SMOKE_COMMAND: &str = "cargo test -p spot-lab --lib mcp::closure_tests::real_http_mcp_closes_research_runtime_contracts";

/// Evidence bound to the exact compiled source and toolchain identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerifiedBuildReceipt {
    receipt_version: u32,
    code_revision: String,
    source_digest: String,
    lockfile_digest: String,
    schema_version: String,
    engine_version: String,
    toolchain: String,
    gates: VerificationGates,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VerificationGates {
    canonical_ci: GateProof,
    public_schema: GateProof,
    critical_smoke: GateProof,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GateProof {
    command: String,
    status: GateStatus,
    log_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum GateStatus {
    #[serde(rename = "PASS")]
    Pass,
}

#[derive(Clone, Copy)]
struct CompiledIdentity<'a> {
    code_revision: &'a str,
    source_digest: &'a str,
    lockfile_digest: &'a str,
    schema_version: &'a str,
    engine_version: &'a str,
    toolchain: &'a str,
}

impl CompiledIdentity<'static> {
    fn current() -> Self {
        Self {
            code_revision: env!("SPOT_LAB_GIT_REVISION"),
            source_digest: env!("SPOT_LAB_SOURCE_SHA256"),
            lockfile_digest: env!("SPOT_LAB_LOCK_SHA256"),
            schema_version: crate::contracts::SCHEMA_VERSION,
            engine_version: crate::contracts::ENGINE_VERSION,
            toolchain: env!("SPOT_LAB_TOOLCHAIN"),
        }
    }
}

/// Return the embedded receipt only when every required gate and compiled
/// identity validates. Missing or untrusted evidence remains unverified.
#[must_use]
pub(crate) fn verified_build_receipt() -> Option<VerifiedBuildReceipt> {
    validate_receipt(
        option_env!("SPOT_LAB_VERIFICATION_RECEIPT")?,
        CompiledIdentity::current(),
    )
}

fn validate_receipt(raw: &str, compiled: CompiledIdentity<'_>) -> Option<VerifiedBuildReceipt> {
    if raw.is_empty() || raw.len() > MAX_RECEIPT_BYTES || !is_build_revision(compiled.code_revision)
    {
        return None;
    }
    let receipt: VerifiedBuildReceipt = serde_json::from_str(raw).ok()?;
    let identity_matches = receipt.receipt_version == RECEIPT_VERSION
        && receipt.code_revision == compiled.code_revision
        && receipt.source_digest == compiled.source_digest
        && receipt.lockfile_digest == compiled.lockfile_digest
        && receipt.schema_version == compiled.schema_version
        && receipt.engine_version == compiled.engine_version
        && receipt.toolchain == compiled.toolchain;
    let digests_valid = is_sha256(&receipt.source_digest)
        && is_sha256(&receipt.lockfile_digest)
        && [
            &receipt.gates.canonical_ci,
            &receipt.gates.public_schema,
            &receipt.gates.critical_smoke,
        ]
        .into_iter()
        .all(|gate| is_sha256(&gate.log_sha256));
    let commands_match = receipt.gates.canonical_ci.command == CANONICAL_CI_COMMAND
        && receipt.gates.public_schema.command == PUBLIC_SCHEMA_COMMAND
        && receipt.gates.critical_smoke.command == CRITICAL_SMOKE_COMMAND;
    (identity_matches && digests_valid && commands_match).then_some(receipt)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_build_revision(value: &str) -> bool {
    let sha = value.strip_suffix("-dirty").unwrap_or(value);
    sha.len() == 40
        && sha
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";
    const SOURCE: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const LOCK: &str = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    const LOG: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    fn identity() -> CompiledIdentity<'static> {
        CompiledIdentity {
            code_revision: REVISION,
            source_digest: SOURCE,
            lockfile_digest: LOCK,
            schema_version: "1.0",
            engine_version: "engine-v1",
            toolchain: "rustc fixture",
        }
    }

    fn receipt() -> Value {
        json!({
            "receipt_version": 1,
            "code_revision": REVISION,
            "source_digest": SOURCE,
            "lockfile_digest": LOCK,
            "schema_version": "1.0",
            "engine_version": "engine-v1",
            "toolchain": "rustc fixture",
            "gates": {
                "canonical_ci": {"command":CANONICAL_CI_COMMAND, "status": "PASS", "log_sha256": LOG},
                "public_schema": {"command":PUBLIC_SCHEMA_COMMAND, "status": "PASS", "log_sha256": LOG},
                "critical_smoke": {"command":CRITICAL_SMOKE_COMMAND, "status": "PASS", "log_sha256": LOG}
            }
        })
    }

    fn accepts(value: &Value, compiled: CompiledIdentity<'_>) -> bool {
        validate_receipt(
            &serde_json::to_string(value).expect("serialize receipt"),
            compiled,
        )
        .is_some()
    }

    #[test]
    fn accepts_exact_complete_receipt() {
        assert!(accepts(&receipt(), identity()));
    }

    #[test]
    fn rejects_partial_unknown_mismatched_or_invalid_receipts() {
        let mut cases = Vec::new();

        let mut value = receipt();
        value["receipt_version"] = json!(2);
        cases.push(("receipt version", value));
        for (field, replacement) in [
            (
                "code_revision",
                json!("fedcba9876543210fedcba9876543210fedcba98"),
            ),
            ("source_digest", json!(LOCK)),
            ("lockfile_digest", json!(SOURCE)),
            ("schema_version", json!("2.0")),
            ("engine_version", json!("engine-v2")),
            ("toolchain", json!("rustc other")),
        ] {
            let mut value = receipt();
            value[field] = replacement;
            cases.push((field, value));
        }
        for gate in ["canonical_ci", "public_schema", "critical_smoke"] {
            let mut wrong_command = receipt();
            wrong_command["gates"][gate]["command"] = json!("echo PASS");
            cases.push(("wrong gate command", wrong_command));
            let mut unknown = receipt();
            unknown["gates"][gate]["status"] = json!("UNKNOWN");
            cases.push((gate, unknown));
            let mut bad_digest = receipt();
            bad_digest["gates"][gate]["log_sha256"] = json!("not-a-digest");
            cases.push((gate, bad_digest));
        }
        let mut missing_gate = receipt();
        missing_gate["gates"]
            .as_object_mut()
            .expect("gates object")
            .remove("critical_smoke");
        cases.push(("missing gate", missing_gate));
        let mut extra = receipt();
        extra["unexpected"] = json!(true);
        cases.push(("unknown field", extra));
        let mut extra_gate_field = receipt();
        extra_gate_field["gates"]["canonical_ci"]["log_path"] = json!("private.log");
        cases.push(("unknown gate field", extra_gate_field));
        let mut invalid_identity_digest = receipt();
        invalid_identity_digest["source_digest"] = json!("not-a-digest");
        cases.push(("invalid identity digest", invalid_identity_digest));

        for (name, value) in cases {
            assert!(!accepts(&value, identity()), "accepted {name}");
        }
        let mut unknown_revision = identity();
        unknown_revision.code_revision = "unknown";
        assert!(!accepts(&receipt(), unknown_revision));
        assert!(validate_receipt("{", identity()).is_none());
        assert!(validate_receipt(&"x".repeat(MAX_RECEIPT_BYTES + 1), identity()).is_none());
    }
}
