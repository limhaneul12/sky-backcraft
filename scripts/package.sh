#!/bin/sh
# Assemble the downloadable macOS app bundle from release binaries.
# Usage: scripts/package.sh   (run after `cargo build --release`)
set -eu

VERSION=$(cargo metadata --no-deps --format-version 1 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['packages'][0]['version'])" \
  2>/dev/null || echo "0.1.0")
APP=dist/SkyBackcraft.app
CONTENTS="$APP/Contents"

rm -rf "$APP"
mkdir -p "$CONTENTS/MacOS" "$CONTENTS/Resources"

cat > "$CONTENTS/Info.plist" << 'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>SkyBackcraft</string>
  <key>CFBundleDisplayName</key><string>Sky Backcraft</string>
  <key>CFBundleIdentifier</key><string>store.skygptcodex.skybackcraft</string>
  <key>CFBundleExecutable</key><string>SkyBackcraftSetup</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>VERSION_PLACEHOLDER</string>
  <key>CFBundleVersion</key><string>VERSION_PLACEHOLDER</string>
  <key>LSMinimumSystemVersion</key><string>12.0</string>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
PLIST
printf '%s' "$VERSION" | xxd -p | head -1 > /dev/null # VERSION sanity
sed -i '' "s/VERSION_PLACEHOLDER/$VERSION/g" "$CONTENTS/Info.plist"

cp target/release/sky-backcraft-setup "$CONTENTS/MacOS/SkyBackcraftSetup"
cp target/release/spot-lab "$CONTENTS/MacOS/spot-lab"
chmod +x "$CONTENTS/MacOS/SkyBackcraftSetup" "$CONTENTS/MacOS/spot-lab"

printf 'APPL????' > "$CONTENTS/PkgInfo"

(cd dist && zip -qry SkyBackcraft-macos.zip SkyBackcraft.app)
echo "package: dist/SkyBackcraft-macos.zip"
shasum -a 256 dist/SkyBackcraft-macos.zip
