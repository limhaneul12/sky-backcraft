use sha2::{Digest, Sha256};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

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
