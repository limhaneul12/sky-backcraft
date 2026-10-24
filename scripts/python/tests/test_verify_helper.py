"""Behavior tests for the isolated packaged-helper gate."""

from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import verify_helper as verifier


def migration_fixture(root, count=2):
    source = root / "crates/lab/src/storage"
    migrations = source / "migrations"
    migrations.mkdir(parents=True)
    entries = []
    for version in range(1, count + 1):
        name = f"{version:04d}_migration_{version}"
        filename = f"{name}.sql"
        (migrations / filename).write_text(f"SELECT {version};\n")
        entries.append(
            f'({version}, "{name}", include_str!("migrations/{filename}"))'
        )
    (source / "migrations.rs").write_text(
        "const MIGRATIONS: &[(&str, &str)] = &[\n" + ",\n".join(entries) + "\n];"
    )
    return root


class HelperGateTests(unittest.TestCase):
    def test_expected_binary_requires_matching_release_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            packaged = Path(directory) / "packaged"
            release = Path(directory) / "release"
            packaged.write_bytes(b"same helper bytes")
            release.write_bytes(b"same helper bytes")
            self.assertEqual(verifier._binary_identity(packaged, release)[1],
                             verifier._binary_identity(release)[1])
            release.write_bytes(b"different helper bytes")
            with self.assertRaisesRegex(ValueError, "differs from release"):
                verifier._binary_identity(packaged, release)

    def test_lab_status_rejects_source_or_lock_mismatch(self):
        for field in ("source_digest", "lockfile_digest"):
            with self.subTest(field=field):
                status = {"source_digest": "source", "lockfile_digest": "lock"}
                status[field] = "stale"
                with self.assertRaisesRegex(ValueError, field):
                    verifier._check_status(status, "source", "lock")

    def test_migration_registry_rejects_unregistered_sql_and_stale_database(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = migration_fixture(Path(directory))
            migrations = verifier._migration_contract(workspace)
            database_path = Path(directory) / "lab.sqlite"
            with closing(sqlite3.connect(database_path)) as database, database:
                database.execute(
                    "CREATE TABLE schema_migrations "
                    "(version INTEGER, name TEXT, checksum TEXT, applied_at TEXT)"
                )
                version, name, _, checksum = migrations[0]
                database.execute(
                    "INSERT INTO schema_migrations VALUES (?, ?, ?, ?)",
                    (version, name, checksum, "2026-10-21T00:00:00Z"),
                )
            with self.assertRaisesRegex(ValueError, "does not match"):
                verifier._migration_history(database_path, migrations)
            orphan = workspace / "crates/lab/src/storage/migrations/0003_orphan.sql"
            orphan.write_text("SELECT 3;\n")
            with self.assertRaisesRegex(ValueError, "unregistered"):
                verifier._migration_contract(workspace)

    def test_restart_reuses_clean_root_and_preserves_readback_history(self):
        workspace = Path(verifier.__file__).resolve().parents[2]
        migrations = verifier._migration_contract(workspace)
        source_digest = "a" * 64
        lock_digest = verifier._sha256(workspace / "Cargo.lock")
        roots = []
        environments = []

        class Client:
            url = "http://127.0.0.1:12345/mcp"

            def call(self, name, arguments):
                if name != "lab_status" or arguments != {}:
                    raise AssertionError("unexpected MCP capability")
                return {"source_digest": source_digest, "lockfile_digest": lock_digest}

        class Server:
            def __init__(self, binary, root, log_path, public_no_auth, environment):
                self.root = Path(root)
                self.database_path = self.root / "lab.sqlite"
                roots.append(self.root)
                environments.append(environment)
                if not self.database_path.exists():
                    with closing(sqlite3.connect(self.database_path)) as database, database:
                        database.execute(
                            "CREATE TABLE schema_migrations "
                            "(version INTEGER, name TEXT, checksum TEXT, applied_at TEXT)"
                        )
                        database.executemany(
                            "INSERT INTO schema_migrations VALUES (?, ?, ?, ?)",
                            [(version, name, checksum, "2026-10-21T00:00:00Z")
                             for version, name, _, checksum in migrations],
                        )

            def start(self):
                return Client()

            def stop(self):
                pass

        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "spot-lab"
            binary.write_bytes(b"test helper")
            binary.chmod(binary.stat().st_mode | 0o111)
            with patch.object(verifier, "IsolatedServer", Server), patch.object(
                verifier, "current_source_digest", return_value=source_digest
            ):
                result = verifier.verify_helper(binary)

        self.assertEqual(len(roots), 2)
        self.assertEqual(roots[0], roots[1])
        self.assertTrue(result["restart_preserved_migration_history"])
        self.assertEqual(result["migration_schema_version"], migrations[-1][0])
        allowed = {"PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE"}
        self.assertTrue(all(set(environment) <= allowed for environment in environments))
        self.assertTrue(all(environment["HOME"] == str(root.parent)
                            for environment, root in zip(environments, roots)))


if __name__ == "__main__":
    unittest.main()
