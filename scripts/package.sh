#!/bin/sh
# Build the Rust backend and assemble the native macOS menu bar app.
set -eu

REPOSITORY_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TARGET_DIRECTORY=${CARGO_TARGET_DIR:-/Volumes/256.SSD/RustBuild/sky-backcraft}
# A stale /Volumes directory is not evidence that the external disk is mounted.
# Refuse to create build artifacts on the internal disk through that directory.
verify_build_volume() {
python3 - "$TARGET_DIRECTORY" <<'PY'
import sys
from pathlib import Path
target = Path(sys.argv[1])
if target.is_absolute() and len(target.parts) > 2 and target.parts[1] == "Volumes":
    volume = Path("/Volumes") / target.parts[2]
    if not volume.is_dir() or volume.stat().st_dev == Path("/Volumes").stat().st_dev:
        raise SystemExit(f"Build volume is not mounted: {volume}")
PY
}
verify_build_volume
APP_PATH="$REPOSITORY_ROOT/dist/Sky Backcraft.app"
VERSION=$(cd "$REPOSITORY_ROOT" && cargo metadata --no-deps --format-version 1 \
  | python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] == "spot-lab"))')

mkdir -p "$REPOSITORY_ROOT/dist"
CARGO_TARGET_DIR="$TARGET_DIRECTORY" cargo build --manifest-path "$REPOSITORY_ROOT/Cargo.toml" --release --bin spot-lab
verify_build_volume

# FileProvider can asynchronously reapply quarantine inside the repository.
# Validate locally generated executables outside that surface before export.
STAGING_DIRECTORY=$(mktemp -d "${TMPDIR:-/tmp}/sky-backcraft-package.XXXXXX")
STAGING_PATH="$STAGING_DIRECTORY/Sky Backcraft.app"
CONTENTS="$STAGING_PATH/Contents"
trap 'rm -rf "$STAGING_DIRECTORY"' EXIT HUP INT TERM
mkdir -p "$CONTENTS/MacOS" "$CONTENTS/Resources"
cp "$REPOSITORY_ROOT/macos/Info.plist" "$CONTENTS/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleShortVersionString $VERSION" "$CONTENTS/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleVersion $VERSION" "$CONTENTS/Info.plist"

xcrun swiftc -parse-as-library -swift-version 5 -warnings-as-errors \
  -target "$(uname -m)-apple-macos12.0" -framework AppKit -framework Security \
  "$REPOSITORY_ROOT"/macos/*.swift -o "$CONTENTS/MacOS/SkyBackcraft"
cp "$TARGET_DIRECTORY/release/spot-lab" "$CONTENTS/MacOS/spot-lab"
chmod 755 "$CONTENTS/MacOS/SkyBackcraft" "$CONTENTS/MacOS/spot-lab"
printf 'APPL????' > "$CONTENTS/PkgInfo"
# Keep the Rust linker's nested executable signature intact. Deep signing
# rewrites it and can prevent macOS from starting the helper before Rust main.
codesign --verify --strict "$CONTENTS/MacOS/spot-lab"
if xattr -p com.apple.quarantine "$STAGING_PATH" >/dev/null 2>&1; then
  xattr -dr com.apple.quarantine "$STAGING_PATH"
fi
codesign --force --sign - "$STAGING_PATH"
verify_bundle() {
  python3 - "$1" "$TARGET_DIRECTORY/release/spot-lab" <<'PY'
import filecmp
from pathlib import Path
import subprocess
import sys
app = Path(sys.argv[1])
helper = app / "Contents/MacOS/spot-lab"
subprocess.run(["codesign", "--verify", "--deep", "--strict", str(app)], check=True, timeout=15)
if not filecmp.cmp(sys.argv[2], helper, shallow=False):
    raise SystemExit("Packaged helper differs from the release binary")
subprocess.run([str(helper), "--help"], check=True, stdout=subprocess.DEVNULL, timeout=15)
subprocess.run([str(app / "Contents/MacOS/SkyBackcraft"), "--self-test"], check=True, timeout=15)
PY
}
verify_bundle "$STAGING_PATH"
ARCHIVE_PATH="$STAGING_DIRECTORY/SkyBackcraft-macos.zip"
ditto -c -k --sequesterRsrc --keepParent "$STAGING_PATH" "$ARCHIVE_PATH"
ditto -x -k "$ARCHIVE_PATH" "$STAGING_DIRECTORY/unpacked"
verify_bundle "$STAGING_DIRECTORY/unpacked/Sky Backcraft.app"
# Publish the verified archive bytes before FileProvider can retag the app copy.
mv "$ARCHIVE_PATH" "$REPOSITORY_ROOT/dist/SkyBackcraft-macos.zip"

if [ -e "$APP_PATH" ]; then
  BACKUP_PATH="$REPOSITORY_ROOT/dist/Sky Backcraft-backup-$(date +%Y%m%d-%H%M%S)-$$.app"
  mv "$APP_PATH" "$BACKUP_PATH"
  echo "backup: $BACKUP_PATH"
fi
mv "$STAGING_PATH" "$APP_PATH"
# The unpacked dist app is a convenience copy. The verified ZIP is authoritative;
# FileProvider can asynchronously retag this copy after it leaves staging.
if xattr -p com.apple.quarantine "$APP_PATH" >/dev/null 2>&1; then
  xattr -dr com.apple.quarantine "$APP_PATH"
fi

echo "package: $REPOSITORY_ROOT/dist/SkyBackcraft-macos.zip"
shasum -a 256 "$REPOSITORY_ROOT/dist/SkyBackcraft-macos.zip"
