use sha2::{Digest, Sha256};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

const BUILD_REVISION_ENV: &str = "SKY_BACKCRAFT_BUILD_REVISION";
const VERIFICATION_RECEIPT_ENV: &str = "SKY_BACKCRAFT_VERIFICATION_RECEIPT";
const MAX_VERIFICATION_RECEIPT_BYTES: usize = 8_192;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crate_root = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let workspace = crate_root
        .parent()
        .and_then(Path::parent)
        .ok_or("workspace parent missing")?;
    let mut sources = Vec::new();
    gather(&crate_root.join("src"), &mut sources)?;
    sources.extend([
        crate_root.join("build.rs"),
        crate_root.join("Cargo.toml"),
        workspace.join("Cargo.toml"),
    ]);
    sources.sort();
    let mut hash = Sha256::new();
    for path in sources {
        let relative = path
            .strip_prefix(workspace)?
            .to_str()
            .ok_or("source path is not UTF-8")?;
        hash.update(relative.as_bytes());
        hash.update([0]);
        hash.update(fs::read(&path)?);
        hash.update([0]);
        println!("cargo:rerun-if-changed={}", path.display());
    }
    println!("cargo:rerun-if-changed=src");
    let lock = workspace.join("Cargo.lock");
    let lock_hash = format!("{:x}", Sha256::digest(fs::read(&lock)?));
    println!("cargo:rerun-if-changed={}", lock.display());
    println!("cargo:rerun-if-env-changed={BUILD_REVISION_ENV}");
    println!("cargo:rerun-if-env-changed={VERIFICATION_RECEIPT_ENV}");
    watch_git_identity(workspace);
    let revision = resolve_build_revision(workspace)?;
    println!("cargo:rustc-env=SPOT_LAB_GIT_REVISION={revision}");
    if let Some(receipt) = bounded_verification_receipt() {
        println!("cargo:rustc-env=SPOT_LAB_VERIFICATION_RECEIPT={receipt}");
    }
    println!(
        "cargo:rustc-env=SPOT_LAB_SOURCE_SHA256={:x}",
        hash.finalize()
    );
    println!("cargo:rustc-env=SPOT_LAB_LOCK_SHA256={lock_hash}");
    let compiler = env::var("RUSTC").unwrap_or_else(|_| "rustc".into());
    let version = Command::new(compiler).arg("--version").output()?;
    if !version.status.success() {
        return Err("compiler version query failed".into());
    }
    println!(
        "cargo:rustc-env=SPOT_LAB_TOOLCHAIN={}",
        String::from_utf8(version.stdout)?.trim()
    );
    Ok(())
}

fn resolve_build_revision(workspace: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let git_revision = match git_output(workspace, &["rev-parse", "HEAD"])
        .filter(|value| is_full_git_sha(value))
    {
        Some(sha) => {
            let revision = if git_is_dirty(workspace)? {
                format!("{sha}-dirty")
            } else {
                sha
            };
            Some(revision)
        }
        None => None,
    };
    let supplied = env::var(BUILD_REVISION_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    if let Some(value) = supplied.as_deref()
        && !is_build_revision(value)
    {
        return Err(format!(
            "{BUILD_REVISION_ENV} must be a full lowercase Git SHA with an optional -dirty suffix"
        )
        .into());
    }
    match (git_revision, supplied) {
        (Some(actual), Some(supplied)) if actual != supplied => Err(format!(
            "{BUILD_REVISION_ENV} does not match the source checkout ({supplied} != {actual})"
        )
        .into()),
        (Some(actual), _) => Ok(actual),
        (None, Some(supplied)) => Ok(supplied),
        (None, None) => Ok("unknown".to_owned()),
    }
}

fn git_is_dirty(workspace: &Path) -> Result<bool, Box<dyn std::error::Error>> {
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .current_dir(workspace)
        .output()?;
    if !output.status.success() {
        return Err("git status failed while resolving the build revision".into());
    }
    Ok(!output.stdout.is_empty())
}

fn git_output(workspace: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(workspace)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn watch_git_identity(workspace: &Path) {
    let Some(git_dir) = git_output(workspace, &["rev-parse", "--git-dir"]) else {
        return;
    };
    let git_dir = if Path::new(&git_dir).is_absolute() {
        PathBuf::from(git_dir)
    } else {
        workspace.join(git_dir)
    };
    let common_dir = git_output(workspace, &["rev-parse", "--git-common-dir"])
        .map(PathBuf::from)
        .map_or_else(
            || git_dir.clone(),
            |path| {
                if path.is_absolute() {
                    path
                } else {
                    workspace.join(path)
                }
            },
        );
    let head = git_dir.join("HEAD");
    println!("cargo:rerun-if-changed={}", head.display());
    println!(
        "cargo:rerun-if-changed={}",
        common_dir.join("packed-refs").display()
    );
    println!("cargo:rerun-if-changed={}", git_dir.join("index").display());
    if let Ok(contents) = fs::read_to_string(head)
        && let Some(reference) = contents.trim().strip_prefix("ref: ")
    {
        println!(
            "cargo:rerun-if-changed={}",
            common_dir.join(reference).display()
        );
    }
    watch_tracked_files(workspace);
}

fn watch_tracked_files(workspace: &Path) {
    let Ok(output) = Command::new("git")
        .args(["ls-files", "-z"])
        .current_dir(workspace)
        .output()
    else {
        return;
    };
    if !output.status.success() {
        return;
    }
    for relative in output.stdout.split(|byte| *byte == 0) {
        if let Ok(relative) = std::str::from_utf8(relative)
            && !relative.is_empty()
        {
            println!(
                "cargo:rerun-if-changed={}",
                workspace.join(relative).display()
            );
        }
    }
}

fn bounded_verification_receipt() -> Option<String> {
    env::var(VERIFICATION_RECEIPT_ENV).ok().filter(|receipt| {
        !receipt.is_empty()
            && receipt.len() <= MAX_VERIFICATION_RECEIPT_BYTES
            && !receipt.bytes().any(|byte| byte == b'\r' || byte == b'\n')
    })
}

fn is_full_git_sha(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_build_revision(value: &str) -> bool {
    is_full_git_sha(value.strip_suffix("-dirty").unwrap_or(value))
}

fn gather(root: &Path, output: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            gather(&entry.path(), output)?;
        } else if entry
            .path()
            .extension()
            .is_some_and(|ext| ext == "rs" || ext == "sql")
        {
            output.push(entry.path());
        }
    }
    Ok(())
}
