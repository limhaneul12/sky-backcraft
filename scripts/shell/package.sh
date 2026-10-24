#!/bin/sh
# Build the Rust backend and assemble the native macOS menu bar app.
set -eu

REPOSITORY_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
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
OUTPUT_DIRECTORY=${SKY_BACKCRAFT_PACKAGE_OUTPUT_DIR:-"$REPOSITORY_ROOT/dist"}
case "$OUTPUT_DIRECTORY" in
  /*) ;;
  *) echo "Package output directory must be absolute: $OUTPUT_DIRECTORY" >&2; exit 1 ;;
esac
APP_PATH="$OUTPUT_DIRECTORY/Sky Backcraft.app"
python3 "$REPOSITORY_ROOT/scripts/python/dist_outputs.py" "$OUTPUT_DIRECTORY" --check
VERSION=$(cd "$REPOSITORY_ROOT" && cargo metadata --locked --no-deps --format-version 1 \
  | python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] == "spot-lab"))')

mkdir -p "$OUTPUT_DIRECTORY"
CARGO_TARGET_DIR="$TARGET_DIRECTORY" cargo build --locked --manifest-path "$REPOSITORY_ROOT/Cargo.toml" --release --bin spot-lab
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
  -target "$(uname -m)-apple-macos12.0" -framework AppKit -framework Security -lsqlite3 \
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
  python3 - "$1" <<'PY'
from pathlib import Path
import subprocess
import sys
app = Path(sys.argv[1])
subprocess.run(["codesign", "--verify", "--deep", "--strict", str(app)], check=True, timeout=15)
subprocess.run([str(app / "Contents/MacOS/SkyBackcraft"), "--self-test"], check=True, timeout=15)
PY
}
verify_bundle "$STAGING_PATH"
python3 "$REPOSITORY_ROOT/scripts/python/verify_helper.py" \
  --binary "$CONTENTS/MacOS/spot-lab" --expected-binary "$TARGET_DIRECTORY/release/spot-lab"
ARCHIVE_PATH="$STAGING_DIRECTORY/SkyBackcraft-macos.zip"
ditto -c -k --sequesterRsrc --keepParent "$STAGING_PATH" "$ARCHIVE_PATH"
ditto -x -k "$ARCHIVE_PATH" "$STAGING_DIRECTORY/unpacked"
verify_bundle "$STAGING_DIRECTORY/unpacked/Sky Backcraft.app"
python3 "$REPOSITORY_ROOT/scripts/python/verify_helper.py" \
  --binary "$STAGING_DIRECTORY/unpacked/Sky Backcraft.app/Contents/MacOS/spot-lab" \
  --expected-binary "$TARGET_DIRECTORY/release/spot-lab"
# Validate both deliverables before replacing either preceding output. The app
# archive is temporary verification material, not a retained dist artifact.
MCP_URL=$(python3 - "$REPOSITORY_ROOT/plugin-marketplace/plugins/sky-backcraft/mcp.json" <<'PY'
import json
import sys
with open(sys.argv[1]) as source:
    print(json.load(source)["mcpServers"]["sky-backcraft"]["url"])
PY
)
PLUGIN_PATH="$STAGING_DIRECTORY/SkyBackcraft-plugin-marketplace.zip"
python3 "$REPOSITORY_ROOT/scripts/python/package-plugin.py" --mcp-url "$MCP_URL" --output "$PLUGIN_PATH"
python3 "$REPOSITORY_ROOT/scripts/python/dist_outputs.py" "$OUTPUT_DIRECTORY" \
  --app "$STAGING_PATH" --plugin "$PLUGIN_PATH"
# FileProvider can asynchronously retag the locally generated app copy.
if xattr -p com.apple.quarantine "$APP_PATH" >/dev/null 2>&1; then
  xattr -dr com.apple.quarantine "$APP_PATH"
fi

echo "app: $APP_PATH"
echo "plugin: $OUTPUT_DIRECTORY/SkyBackcraft-plugin-marketplace.zip"
shasum -a 256 "$OUTPUT_DIRECTORY/SkyBackcraft-plugin-marketplace.zip"
