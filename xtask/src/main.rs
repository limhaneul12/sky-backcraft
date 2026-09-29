use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

const NORMAL_RULES: &[&str] = &[
    "README.md",
    "00-overview.md",
    "01-boundary.md",
    "02-workspace-crate-rules.md",
    concat!("03-deterministic-state-", "machine-rules.md"),
    "04-ffi-interop-boundary-rules.md",
    "05-error-panic-rules.md",
    "06-testing-verification-rules.md",
    "07-performance-memory-rules.md",
    "08-observability-operations-rules.md",
    "09-module-layout-rules.md",
];

const TYPE_RULES: &[&str] = &[
    "README.md",
    "01-domain-newtype-invariant-rules.md",
    "02-enum-state-sum-type-rules.md",
    "03-option-result-outcome-rules.md",
    "04-trait-generic-bound-rules.md",
    "05-ownership-borrow-lifetime-rules.md",
    concat!("06-smart-pointer-interior-", "mutability-rules.md"),
    concat!("07-send-sync-thread-safety-", "type-rules.md"),
    "08-conversion-parsing-cast-rules.md",
    concat!("09-public-api-type-", "evolution-rules.md"),
    "10-unsafe-type-invariant-rules.md",
];

const ASYNC_RULES: &[&str] = &[
    "README.md",
    "01-execution-model-runtime-rules.md",
    concat!("02-task-ownership-", "structured-concurrency-rules.md"),
    "03-cancellation-timeout-select-rules.md",
    concat!("04-bounded-concurrency-", "backpressure-rules.md"),
    "05-shared-state-lock-channel-rules.md",
    "06-blocking-cpu-parallelism-rules.md",
    "07-send-sync-local-task-rules.md",
    "08-async-error-shutdown-rules.md",
    "09-async-testing-verification-rules.md",
    concat!("10-async-observability-", "performance-rules.md"),
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct ToolchainPolicy {
    channel: Option<String>,
}

impl ToolchainPolicy {
    fn load(root: &Path) -> Result<Self, String> {
        let path = root.join("rust-toolchain.toml");
        if !path.is_file() {
            return Ok(Self { channel: None });
        }
        let manifest = read_toml(&path)?;
        let channel = manifest
            .get("toolchain")
            .and_then(toml::Value::as_table)
            .and_then(|table| table.get("channel"))
            .and_then(toml::Value::as_str)
            .ok_or_else(|| format!("{} must define [toolchain].channel", path.display()))?;
        Ok(Self {
            channel: Some(channel.to_owned()),
        })
    }

    fn command(&self, program: &str) -> Command {
        match &self.channel {
            Some(channel) => {
                let mut command = Command::new("rustup");
                command.args(["run", channel, program]);
                command
            }
            None => Command::new(program),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RustVersionPolicy {
    CurrentStable,
    Msrv(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum UnsafePolicy {
    Forbid,
    ApprovedCrates(BTreeSet<String>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HarnessPolicyOrigin {
    PortableBaseline,
    PrivateHarness,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HarnessPolicy {
    origin: HarnessPolicyOrigin,
    rust_version: RustVersionPolicy,
    supply_chain: bool,
    unsafe_policy: UnsafePolicy,
}

impl HarnessPolicy {
    fn load(root: &Path) -> Result<Self, String> {
        let agents = root.join(".agents");
        match fs::metadata(&agents) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(format!("{} must be a directory", agents.display()));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self {
                    origin: HarnessPolicyOrigin::PortableBaseline,
                    rust_version: RustVersionPolicy::CurrentStable,
                    supply_chain: false,
                    unsafe_policy: UnsafePolicy::Forbid,
                });
            }
            Err(error) => {
                return Err(format!(
                    "failed to inspect private harness bundle {}: {error}",
                    agents.display()
                ));
            }
        }

        let config = read_toml(&harness_root(root).join("HARNESS.toml"))?;
        let toolchain = required_table(&config, "toolchain")?;
        let rust_version = match required_str(toolchain, "policy")? {
            "current-stable" => RustVersionPolicy::CurrentStable,
            "msrv" => RustVersionPolicy::Msrv(required_str(toolchain, "msrv")?.to_owned()),
            other => return Err(format!("unsupported toolchain.policy: {other}")),
        };

        let verification = required_table(&config, "verification")?;
        let supply_chain = required_bool(verification, "supply_chain")?;

        let unsafe_table = required_table(&config, "unsafe")?;
        let unsafe_policy = match required_str(unsafe_table, "mode")? {
            "forbid" => UnsafePolicy::Forbid,
            "approved-crates" => {
                let values = unsafe_table
                    .get("approved_crates")
                    .and_then(toml::Value::as_array)
                    .ok_or_else(|| "unsafe.approved_crates must be an array".to_owned())?;
                let crates = values
                    .iter()
                    .map(|value| {
                        value.as_str().map(str::to_owned).ok_or_else(|| {
                            "unsafe.approved_crates entries must be strings".to_owned()
                        })
                    })
                    .collect::<Result<BTreeSet<_>, _>>()?;
                if crates.is_empty() {
                    return Err(
                        "unsafe.mode=approved-crates requires at least one approved crate"
                            .to_owned(),
                    );
                }
                UnsafePolicy::ApprovedCrates(crates)
            }
            other => return Err(format!("unsupported unsafe.mode: {other}")),
        };

        Ok(Self {
            origin: HarnessPolicyOrigin::PrivateHarness,
            rust_version,
            supply_chain,
            unsafe_policy,
        })
    }
}

fn main() {
    let command = std::env::args().nth(1).unwrap_or_else(|| "help".to_owned());
    let result = match command.as_str() {
        "rules" => rules(),
        "fmt" => fmt(),
        "check" => check(),
        "test" => test(),
        "ci" => ci(),
        "deps" => deps(),
        "doctor" => doctor(),
        "extended" => extended(),
        "perf" => perf(),
        "help" | "-h" | "--help" => {
            help();
            Ok(())
        }
        other => Err(format!("unknown xtask command: {other}")),
    };
    if let Err(error) = result {
        eprintln!("xtask: {error}");
        std::process::exit(1);
    }
}

fn help() {
    println!("cargo xtask <rules|fmt|check|test|ci|deps|doctor|extended|perf>");
}

fn workspace_root() -> Result<PathBuf, String> {
    let current_dir = std::env::current_dir()
        .map_err(|error| format!("failed to read current directory: {error}"))?;
    for ancestor in current_dir.ancestors() {
        let path = ancestor.join("Cargo.toml");
        if path.is_file() && read_toml(&path)?.get("workspace").is_some() {
            return Ok(ancestor.to_path_buf());
        }
    }
    Err(format!(
        "could not locate workspace root from {}",
        current_dir.display()
    ))
}

fn harness_root(root: &Path) -> PathBuf {
    root.join(".agents").join("rust_dev_harness")
}

fn read_toml(path: &Path) -> Result<toml::Value, String> {
    let source = fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    toml::from_str(&source).map_err(|error| format!("invalid TOML {}: {error}", path.display()))
}

fn required_table<'a>(
    value: &'a toml::Value,
    name: &str,
) -> Result<&'a toml::value::Table, String> {
    value
        .get(name)
        .and_then(toml::Value::as_table)
        .ok_or_else(|| format!("missing [{name}] table"))
}

fn required_str<'a>(table: &'a toml::value::Table, name: &str) -> Result<&'a str, String> {
    table
        .get(name)
        .and_then(toml::Value::as_str)
        .ok_or_else(|| format!("missing/non-string field {name}"))
}

fn required_bool(table: &toml::value::Table, name: &str) -> Result<bool, String> {
    table
        .get(name)
        .and_then(toml::Value::as_bool)
        .ok_or_else(|| format!("missing/non-boolean field {name}"))
}

fn require_file(path: &Path) -> Result<(), String> {
    if path.is_file() {
        Ok(())
    } else {
        Err(format!("missing required file: {}", path.display()))
    }
}

fn verify_group(root: &Path, directory: &str, files: &[&str]) -> Result<(), String> {
    let directory = root.join("rules").join(directory);
    for file in files {
        require_file(&directory.join(file))?;
    }
    Ok(())
}

fn tool_version(root: &Path, policy: &ToolchainPolicy, program: &str) -> Result<String, String> {
    let mut command = policy.command(program);
    command.arg("--version").current_dir(root);
    output_line(&mut command, &format!("declared {program} --version"))
}

fn ambient_tool_version(root: &Path, program: &str) -> Result<String, String> {
    let mut command = Command::new(program);
    command.arg("--version").current_dir(root);
    output_line(&mut command, &format!("ambient {program} --version"))
}

fn output_line(command: &mut Command, label: &str) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|error| format!("failed to execute {label}: {error}"))?;
    if !output.status.success() {
        return Err(format!("{label} failed with status {}", output.status));
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|error| format!("{label} returned non-UTF8 output: {error}"))
}

fn verify_declared_toolchain_available(
    root: &Path,
    policy: &ToolchainPolicy,
) -> Result<(), String> {
    if policy.channel.is_some() {
        let _ = tool_version(root, policy, "rustc")?;
        let _ = tool_version(root, policy, "cargo")?;
    }
    Ok(())
}

fn rules() -> Result<(), String> {
    let root = workspace_root()?;
    let harness = harness_root(&root);
    let toolchain = ToolchainPolicy::load(&root)?;
    let policy = HarnessPolicy::load(&root)?;

    if policy.origin == HarnessPolicyOrigin::PrivateHarness {
        for required in [
            "PROJECT_PROFILE.md",
            "HARNESS.toml",
            "rules/README.md",
            "skills/rust-engineering/SKILL.md",
        ] {
            require_file(&harness.join(required))?;
        }
        verify_group(&harness, "normal_dev_rules", NORMAL_RULES)?;
        verify_group(&harness, "type_dev_rules", TYPE_RULES)?;
        verify_group(&harness, "async_dev_rules", ASYNC_RULES)?;
    } else {
        println!("xtask rules: private harness absent; using portable baseline");
    }
    verify_declared_toolchain_available(&root, &toolchain)?;
    verify_cargo_baseline(&root, &policy)?;
    verify_workspace_lints(&root, &toolchain, &policy)?;
    println!("xtask rules: PASS");
    Ok(())
}

fn lint_level<'a>(table: &'a toml::value::Table, name: &str) -> Option<&'a str> {
    match table.get(name)? {
        toml::Value::String(value) => Some(value.as_str()),
        toml::Value::Table(value) => value.get("level").and_then(toml::Value::as_str),
        _ => None,
    }
}

fn verify_cargo_baseline(root: &Path, policy: &HarnessPolicy) -> Result<(), String> {
    let manifest = read_toml(&root.join("Cargo.toml"))?;
    let workspace = required_table(&manifest, "workspace")?;
    if workspace.get("resolver").and_then(toml::Value::as_str) != Some("3") {
        return Err("workspace.resolver must be \"3\"".to_owned());
    }
    let package = workspace
        .get("package")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "root Cargo.toml must define [workspace.package]".to_owned())?;
    if package.get("edition").and_then(toml::Value::as_str) != Some("2024") {
        return Err("workspace.package.edition must be \"2024\"".to_owned());
    }
    if let RustVersionPolicy::Msrv(expected) = &policy.rust_version {
        let actual = package
            .get("rust-version")
            .and_then(toml::Value::as_str)
            .ok_or_else(|| {
                "toolchain.policy=msrv requires workspace.package.rust-version".to_owned()
            })?;
        if actual != expected {
            return Err(format!(
                "MSRV mismatch: HARNESS.toml={expected:?}, Cargo.toml={actual:?}"
            ));
        }
    }

    let lints = workspace
        .get("lints")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "root Cargo.toml must define [workspace.lints]".to_owned())?;
    let rust = lints
        .get("rust")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "root Cargo.toml must define [workspace.lints.rust]".to_owned())?;
    if lint_level(rust, "warnings") != Some("deny") {
        return Err("workspace rust warnings must be deny".to_owned());
    }
    if lint_level(rust, "unsafe_code") != Some("forbid") {
        return Err("workspace unsafe_code must be forbid for safe members".to_owned());
    }
    let clippy = lints
        .get("clippy")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "root Cargo.toml must define [workspace.lints.clippy]".to_owned())?;
    if lint_level(clippy, "all") != Some("deny") {
        return Err("workspace Clippy all group must be deny".to_owned());
    }
    Ok(())
}

fn cargo_metadata(root: &Path, policy: &ToolchainPolicy) -> Result<serde_json::Value, String> {
    let mut command = policy.command("cargo");
    command
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(root);
    let output = command
        .output()
        .map_err(|error| format!("failed to run cargo metadata: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed with status {}",
            output.status
        ));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("cargo metadata returned invalid JSON: {error}"))
}

fn verify_workspace_lints(
    root: &Path,
    toolchain: &ToolchainPolicy,
    policy: &HarnessPolicy,
) -> Result<(), String> {
    let metadata = cargo_metadata(root, toolchain)?;
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "cargo metadata has no packages array".to_owned())?;
    let expected_approved = match &policy.unsafe_policy {
        UnsafePolicy::Forbid => BTreeSet::new(),
        UnsafePolicy::ApprovedCrates(crates) => crates.clone(),
    };
    let mut seen_approved = BTreeSet::new();

    for package in packages {
        let name = package
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "cargo metadata package missing name".to_owned())?;
        let manifest_path = package
            .get("manifest_path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "cargo metadata package missing manifest_path".to_owned())?;
        let manifest = read_toml(Path::new(manifest_path))?;

        if expected_approved.contains(name) {
            verify_approved_unsafe_crate(name, &manifest)?;
            seen_approved.insert(name.to_owned());
            continue;
        }

        let inherited = manifest
            .get("lints")
            .and_then(toml::Value::as_table)
            .and_then(|table| table.get("workspace"))
            .and_then(toml::Value::as_bool);
        if inherited != Some(true) {
            return Err(format!(
                "safe workspace member must use [lints] workspace = true: {manifest_path}"
            ));
        }
    }

    if seen_approved != expected_approved {
        let missing = expected_approved
            .difference(&seen_approved)
            .collect::<Vec<_>>();
        return Err(format!(
            "HARNESS.toml approved unsafe crates not found in workspace: {missing:?}"
        ));
    }
    Ok(())
}

fn verify_approved_unsafe_crate(name: &str, manifest: &toml::Value) -> Result<(), String> {
    let lints = manifest
        .get("lints")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| format!("approved unsafe crate {name:?} must define local [lints]"))?;
    if lints.get("workspace").and_then(toml::Value::as_bool) == Some(true) {
        return Err(format!(
            "approved unsafe crate {name:?} must not inherit workspace unsafe_code=forbid"
        ));
    }
    let rust = lints
        .get("rust")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| format!("approved unsafe crate {name:?} must define [lints.rust]"))?;
    for (lint, expected) in [
        ("warnings", "deny"),
        ("unsafe_code", "allow"),
        ("unsafe_op_in_unsafe_fn", "deny"),
    ] {
        if lint_level(rust, lint) != Some(expected) {
            return Err(format!(
                "approved unsafe crate {name:?} must set {lint}={expected}"
            ));
        }
    }
    Ok(())
}

fn cargo_command(root: &Path) -> Result<Command, String> {
    Ok(ToolchainPolicy::load(root)?.command("cargo"))
}

fn status(command: &mut Command, label: &str) -> Result<(), String> {
    let status = command
        .status()
        .map_err(|error| format!("failed to execute {label}: {error}"))?;
    require_success(status, label)
}

fn require_success(status: ExitStatus, label: &str) -> Result<(), String> {
    if status.success() {
        Ok(())
    } else {
        Err(format!("{label} failed with status {status}"))
    }
}

fn fmt() -> Result<(), String> {
    let root = workspace_root()?;
    let mut command = cargo_command(&root)?;
    command
        .args(["fmt", "--all", "--", "--check"])
        .current_dir(&root);
    status(&mut command, "cargo fmt")
}

fn check() -> Result<(), String> {
    let root = workspace_root()?;
    let mut command = cargo_command(&root)?;
    command
        .args(["check", "--workspace", "--all-targets"])
        .current_dir(&root);
    status(&mut command, "cargo check")?;

    let mut command = cargo_command(&root)?;
    command
        .args(["clippy", "--workspace", "--all-targets"])
        .current_dir(&root);
    status(&mut command, "cargo clippy")
}

fn test() -> Result<(), String> {
    let root = workspace_root()?;
    let mut command = cargo_command(&root)?;
    command
        .args(["test", "--workspace", "--all-targets"])
        .current_dir(&root);
    status(&mut command, "cargo test")
}

fn ci() -> Result<(), String> {
    rules()?;
    fmt()?;
    check()?;
    test()
}

fn deps() -> Result<(), String> {
    let root = workspace_root()?;
    require_file(&root.join("deny.toml"))?;
    let mut command = cargo_command(&root)?;
    command.args(["deny", "check"]).current_dir(&root);
    status(&mut command, "cargo deny check")
}

fn doctor() -> Result<(), String> {
    let root = workspace_root()?;
    let policy = ToolchainPolicy::load(&root)?;
    println!("workspace: {}", root.display());
    println!(
        "declared toolchain: {}",
        policy
            .channel
            .as_deref()
            .unwrap_or("ambient/project-profile policy")
    );
    for program in ["rustc", "cargo"] {
        let ambient = ambient_tool_version(&root, program)?;
        println!("ambient {program}: {ambient}");
        if policy.channel.is_some() {
            let declared = tool_version(&root, &policy, program)?;
            println!("declared {program}: {declared}");
            if declared != ambient {
                println!(
                    "WARNING: ambient {program} differs from declared toolchain; xtask commands still use the declared toolchain"
                );
            }
        }
    }
    verify_declared_toolchain_available(&root, &policy)?;
    rules()
}

fn extended() -> Result<(), String> {
    ci()?;
    let root = workspace_root()?;
    if HarnessPolicy::load(&root)?.supply_chain {
        deps()?;
    }

    let mut command = cargo_command(&root)?;
    command
        .args(["test", "--workspace", "--doc"])
        .current_dir(&root);
    status(&mut command, "cargo test --doc")?;

    let mut command = cargo_command(&root)?;
    command
        .args([
            "--config",
            "build.rustdocflags=[\"-D\",\"warnings\"]",
            "doc",
            "--workspace",
            "--no-deps",
        ])
        .current_dir(&root);
    status(&mut command, "cargo doc")
}

fn perf() -> Result<(), String> {
    let root = workspace_root()?;
    let mut command = cargo_command(&root)?;
    command.args(["bench", "--workspace"]).current_dir(&root);
    status(&mut command, "cargo bench")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temporary_test_root(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock must be after Unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "sky-backcraft-xtask-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[test]
    fn uses_rustup_for_pin() {
        let policy = ToolchainPolicy {
            channel: Some("1.98.0".to_owned()),
        };
        let command = policy.command("cargo");
        assert_eq!(command.get_program(), std::ffi::OsStr::new("rustup"));
    }

    #[test]
    fn no_pin_uses_cargo() {
        let policy = ToolchainPolicy { channel: None };
        let command = policy.command("cargo");
        assert_eq!(command.get_program(), std::ffi::OsStr::new("cargo"));
    }

    #[test]
    fn private_harness_falls_back_only_when_agents_bundle_is_absent() {
        let root = temporary_test_root("portable-harness");
        fs::create_dir(&root).expect("test root should be created");

        assert_eq!(
            HarnessPolicy::load(&root),
            Ok(HarnessPolicy {
                origin: HarnessPolicyOrigin::PortableBaseline,
                rust_version: RustVersionPolicy::CurrentStable,
                supply_chain: false,
                unsafe_policy: UnsafePolicy::Forbid,
            })
        );

        fs::create_dir(root.join(".agents")).expect("partial agents bundle should be created");
        let partial_error =
            HarnessPolicy::load(&root).expect_err("partial bundle must fail closed");
        assert!(partial_error.contains("HARNESS.toml"));

        let harness = harness_root(&root);
        fs::create_dir_all(&harness).expect("harness directory should be created");
        fs::write(harness.join("HARNESS.toml"), "not valid TOML = [")
            .expect("malformed harness config should be written");
        let malformed_error =
            HarnessPolicy::load(&root).expect_err("malformed harness config must fail closed");
        assert!(malformed_error.contains("invalid TOML"));

        fs::remove_dir_all(root).expect("test root should be removed");
    }
}
