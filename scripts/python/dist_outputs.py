"""Publish current build artifacts without retaining previous versions in dist."""
import argparse
import filecmp
import fcntl
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

APP_NAME = "Sky Backcraft.app"
PLUGIN_NAME = "SkyBackcraft-plugin-marketplace.zip"
CANONICAL_DIST = Path(__file__).resolve().parents[2] / "dist"
LEGACY = re.compile(r"(?:goal-(?:delivery|plugin)(?:-v\d+)?|Sky Backcraft-backup-[\d-]+\.app|SkyBackcraft-macos\.zip|\.DS_Store)")


def validate_output_directory(directory, canonical_dist=CANONICAL_DIST):
    directory = Path(directory)
    if directory.is_symlink():
        raise ValueError("Distribution output cannot be a symlink")
    directory = directory.resolve()
    canonical_dist = canonical_dist.resolve()
    if directory != canonical_dist and directory.is_relative_to(canonical_dist):
        raise ValueError("Write latest artifacts directly to dist, not a version subdirectory")
    if canonical_dist.is_relative_to(directory) and directory != canonical_dist:
        raise ValueError("Distribution output cannot be the repository or its ancestor")
    return directory


def check_destination(directory, replacements):
    directory = validate_output_directory(directory)
    entries = list(directory.iterdir()) if directory.exists() else []
    for entry in entries:
        if entry.name not in {APP_NAME, PLUGIN_NAME} and not LEGACY.fullmatch(entry.name):
            raise ValueError(f"Unknown dist entry; move it outside dist before packaging: {entry.name}")
        if entry.is_symlink():
            raise ValueError(f"Distribution entry cannot be a symlink: {entry.name}")
    changing = [p for p in entries if p.name in replacements or LEGACY.fullmatch(p.name)]
    check_running(changing)
    return changing


def check_running(paths):
    processes = [str(Path(p.strip()).resolve()) for p in
                 subprocess.check_output(["ps", "-axo", "comm="], text=True).splitlines()
                 if p.strip().startswith("/")]
    for entry in paths:
        if any(process.strip().startswith(str(entry) + "/") for process in processes):
            raise ValueError(f"Cannot replace a running app/helper; quit Sky Backcraft before packaging: {entry}")


def verify_launch_copies(installed_app, launch_apps):
    """Reject obsolete launch paths without opening the research database."""
    installed_app = Path(installed_app)
    components = ("Contents/MacOS/spot-lab", "Contents/MacOS/SkyBackcraft", "Contents/Info.plist")
    for relative in components:
        if not (installed_app / relative).is_file():
            raise ValueError(f"Incomplete installed app: {installed_app / relative}")
    for launch_app in map(Path, launch_apps):
        if not launch_app.exists() and not launch_app.is_symlink():
            continue
        for relative in components:
            candidate = launch_app / relative
            if not candidate.is_file() or not filecmp.cmp(installed_app / relative, candidate, shallow=False):
                raise ValueError(
                    f"Stale launchable app: {launch_app} ({relative} differs from {installed_app}). "
                    "Archive the obsolete bundle or replace it with the verified installed build; "
                    "do not downgrade the database."
                )


def remove(path):
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()


def publish_outputs(directory, sources):
    directory = validate_output_directory(directory)
    if not sources or not set(sources).issubset({APP_NAME, PLUGIN_NAME}):
        raise ValueError("Publish only the current app and plugin ZIP")
    for name, source in sources.items():
        source = Path(source)
        if source.is_symlink() or source.resolve().is_relative_to(directory):
            raise ValueError("Stage artifacts outside the output directory")
        if not (source.is_dir() if name == APP_NAME else source.is_file()):
            raise ValueError(f"Missing staged artifact: {source}")
    directory.mkdir(parents=True, exist_ok=True)
    # Lock the directory itself: no lock artifact remains in dist, and all
    # cooperating app/plugin publishers share the same lock through rollback.
    descriptor = os.open(directory, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        _publish_locked(directory, sources)
    finally:
        os.close(descriptor)


def _publish_locked(directory, sources):
    check_destination(directory, sources)
    work = Path(tempfile.mkdtemp(prefix=".sky-backcraft-dist-", dir=directory.parent))
    cleanup = True
    moved, installed = [], []
    try:
        (work / "new").mkdir()
        (work / "old").mkdir()
        for name, source in sources.items():
            if name == APP_NAME:
                shutil.copytree(source, work / "new" / name, symlinks=True)
            else:
                shutil.copy2(source, work / "new" / name)
        # Validate again after copying, before touching the preceding build.
        changing = check_destination(directory, sources)
        try:
            for entry in changing:
                os.replace(entry, work / "old" / entry.name)
                moved.append(entry.name)
            # Catch a launch observed during the preflight-to-rename interval;
            # old launch paths are now absent until publication or rollback.
            check_running([directory / name for name in moved] + [work / "old" / name for name in moved])
            for name in sources:
                os.replace(work / "new" / name, directory / name)
                installed.append(name)
        except BaseException:
            try:
                for name in reversed(installed):
                    remove(directory / name)
                for name in reversed(moved):
                    os.replace(work / "old" / name, directory / name)
            except BaseException as error:
                cleanup = False
                raise RuntimeError(f"Restore failed; previous artifacts retained at {work / 'old'}") from error
            raise
    finally:
        if cleanup:
            shutil.rmtree(work)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--app", type=Path)
    parser.add_argument("--plugin", type=Path)
    parser.add_argument("--verify-installed", type=Path)
    parser.add_argument("--launch-app", type=Path, action="append", default=[])
    args = parser.parse_args()
    if args.verify_installed:
        if args.check or args.app or args.plugin:
            parser.error("Installed-app verification is read-only; do not combine it with publication")
        verify_launch_copies(args.verify_installed, [args.directory / APP_NAME, *args.launch_app])
        print(f"PASS installed app and remaining launch paths: {args.verify_installed}")
    elif args.launch_app:
        parser.error("--launch-app requires --verify-installed")
    elif args.check:
        check_destination(args.directory, {APP_NAME, PLUGIN_NAME})
    else:
        sources = {name: path for name, path in [(APP_NAME, args.app), (PLUGIN_NAME, args.plugin)] if path}
        publish_outputs(args.directory, sources)


if __name__ == "__main__":
    main()
