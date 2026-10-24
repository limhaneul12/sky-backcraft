#!/usr/bin/env python3
"""Verify a packaged helper against source identity and a disposable restart."""

import argparse
from contextlib import closing, contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import urllib.parse
import urllib.request

from research_mcp_e2e import IsolatedServer, current_source_digest

_ENV_KEYS = ("PATH", "LANG", "LC_ALL", "LC_CTYPE")
_ENTRY = re.compile(r'\(\s*(\d+)\s*,\s*"([^"]+)"\s*,\s*include_str!\("migrations/([^"/]+\.sql)"\)')
_FILENAME = re.compile(r"(\d{4})_[a-z0-9_]+\.sql")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


@contextmanager
def _loopback_http():
    original = urllib.request.urlopen
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    urllib.request.urlopen = opener.open
    try:
        yield
    finally:
        urllib.request.urlopen = original


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _binary_identity(binary, expected_binary=None):
    binary = Path(binary).resolve(strict=True)
    if not binary.is_file():
        raise ValueError(f"helper is not a file: {binary}")
    digest = _sha256(binary)
    expected = None
    if expected_binary is not None:
        expected = _sha256(Path(expected_binary).resolve(strict=True))
        if digest != expected:
            raise ValueError(f"helper SHA-256 {digest} differs from release {expected}")
    return binary, digest, expected


def _migration_contract(workspace):
    root = Path(workspace) / "crates/lab/src/storage/migrations"
    source = (root.parent / "migrations.rs").read_text(encoding="utf-8")
    entries = []
    for match in _ENTRY.finditer(source):
        version, name, filename = int(match[1]), match[2], match[3]
        filename_match = _FILENAME.fullmatch(filename)
        sql = root / filename
        if (filename_match is None or int(filename_match[1]) != version
                or Path(filename).stem != name or not sql.is_file()):
            raise ValueError(f"invalid or missing registered migration: {version} {filename}")
        entries.append((version, name, filename, _sha256(sql)))
    if not entries or [item[0] for item in entries] != list(range(1, len(entries) + 1)):
        raise ValueError("migration registry is empty or not contiguous from version 1")
    registered = {item[2] for item in entries}
    present = {path.name for path in root.glob("*.sql") if path.is_file()}
    if present != registered:
        raise ValueError(
            f"migration SQL and registry differ: unregistered={sorted(present - registered)}, "
            f"missing={sorted(registered - present)}"
        )
    return tuple(entries)


def _check_status(status, source, lock):
    for key, expected in (("source_digest", source), ("lockfile_digest", lock)):
        if not isinstance(status, dict) or status.get(key) != expected:
            raise ValueError(f"lab_status {key} mismatch: expected {expected}, got {status!r}")


def _migration_history(database_path, migrations):
    uri = Path(database_path).resolve(strict=True).as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as database:
        integrity = [row[0] for row in database.execute("PRAGMA integrity_check")]
        if integrity != ["ok"]:
            raise ValueError(f"SQLite integrity_check failed: {integrity}")
        if list(database.execute("PRAGMA foreign_key_check")):
            raise ValueError("SQLite foreign_key_check found violations")
        history = tuple(tuple(row) for row in database.execute(
            "SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version"
        ))
    expected = tuple((version, name, checksum) for version, name, _, checksum in migrations)
    if tuple(row[:3] for row in history) != expected:
        raise ValueError("schema_migrations does not match registered SQL versions and checksums")
    return history


def verify_helper(binary, expected_binary=None):
    binary, binary_hash, release_hash = _binary_identity(binary, expected_binary)
    if not os.access(binary, os.X_OK):
        raise ValueError(f"helper is not executable: {binary}")

    workspace = Path(__file__).resolve().parents[2]
    source, lock = current_source_digest(), _sha256(workspace / "Cargo.lock")
    migrations = _migration_contract(workspace)
    history = None
    with tempfile.TemporaryDirectory(prefix="sky-backcraft-helper-") as temporary:
        temporary = Path(temporary)
        root = temporary / "data"
        root.mkdir()
        environment = {key: os.environ[key] for key in _ENV_KEYS if key in os.environ}
        environment.update(HOME=str(temporary), TMPDIR=str(temporary))
        for _ in range(2):
            server = IsolatedServer(binary, root, temporary / "helper.log",
                                    public_no_auth=True, environment=environment)
            try:
                with _loopback_http():
                    client = server.start()  # IsolatedServer also performs initialize.
                    if urllib.parse.urlsplit(client.url).hostname != "127.0.0.1":
                        raise ValueError("isolated helper endpoint is not loopback")
                    _check_status(client.call("lab_status", {}), source, lock)
            finally:
                server.stop()
            observed = _migration_history(root / "lab.sqlite", migrations)
            if history is not None and observed != history:
                raise ValueError("schema_migrations history changed across helper restart")
            history = observed

    if current_source_digest() != source or _sha256(workspace / "Cargo.lock") != lock:
        raise ValueError("Rust source or Cargo.lock changed during helper verification")
    return {
        "status": "PASS", "helper_sha256": binary_hash,
        "release_identity_sha256": release_hash, "source_digest": source,
        "lockfile_digest": lock, "migration_schema_version": migrations[-1][0],
        "migration_count": len(history), "restart_preserved_migration_history": True,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path, help="packaged spot-lab helper")
    parser.add_argument("--expected-binary", type=Path, help="release helper identity bytes")
    args = parser.parse_args(argv)
    try:
        result = verify_helper(args.binary, args.expected_binary)
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
