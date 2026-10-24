"""Regression coverage for publishing the two current distribution artifacts."""
import tempfile
import fcntl
import os
import select
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dist_outputs import APP_NAME, PLUGIN_NAME, publish_outputs, validate_output_directory, verify_launch_copies


class DistributionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.dist = self.root / "dist"
        self.dist.mkdir()
        self.app = self.root / "staged" / APP_NAME
        (self.app / "Contents/MacOS").mkdir(parents=True)
        (self.app / "Contents/MacOS/SkyBackcraft").write_text("new app")
        (self.app / "Contents/MacOS/spot-lab").write_text("new helper")
        (self.app / "Contents/Info.plist").write_text("bundle identity")
        self.plugin = self.root / "staged" / PLUGIN_NAME
        self.plugin.write_bytes(b"new plugin")

    def publish(self):
        publish_outputs(self.dist, {APP_NAME: self.app, PLUGIN_NAME: self.plugin})

    def test_repeated_build_replaces_outputs_and_removes_old_versions(self):
        for name in ["goal-delivery-v14", "goal-plugin-v14", "Sky Backcraft-backup-20261014-172232-75986.app"]:
            (self.dist / name).mkdir()
            (self.dist / name / "old").write_text("obsolete")
        (self.dist / "SkyBackcraft-macos.zip").write_bytes(b"old app zip")
        self.publish()
        (self.app / "Contents/MacOS/SkyBackcraft").write_text("newer app")
        self.plugin.write_bytes(b"newer plugin")
        self.publish()
        self.assertEqual({p.name for p in self.dist.iterdir()}, {APP_NAME, PLUGIN_NAME})
        self.assertEqual((self.dist / APP_NAME / "Contents/MacOS/SkyBackcraft").read_text(), "newer app")
        self.assertEqual((self.dist / PLUGIN_NAME).read_bytes(), b"newer plugin")

    def test_failed_publication_restores_both_previous_outputs(self):
        self.publish()
        (self.app / "Contents/MacOS/SkyBackcraft").write_text("replacement app")
        self.plugin.write_bytes(b"replacement plugin")
        import os
        original_replace = os.replace

        def fail_plugin_install(source, destination):
            if Path(destination) == self.dist / PLUGIN_NAME and Path(source).parent.name == "new":
                raise OSError("injected publication failure")
            return original_replace(source, destination)

        with patch("dist_outputs.os.replace", side_effect=fail_plugin_install):
            with self.assertRaisesRegex(OSError, "injected publication failure"):
                self.publish()
        self.assertEqual({p.name for p in self.dist.iterdir()}, {APP_NAME, PLUGIN_NAME})
        self.assertEqual((self.dist / APP_NAME / "Contents/MacOS/SkyBackcraft").read_text(), "new app")
        self.assertEqual((self.dist / PLUGIN_NAME).read_bytes(), b"new plugin")

    def test_running_app_is_preserved_but_plugin_only_update_is_allowed(self):
        self.publish()
        running = str(self.dist / APP_NAME / "Contents/MacOS/spot-lab") + "\n"
        with patch("dist_outputs.subprocess.check_output", return_value=running):
            with self.assertRaisesRegex(ValueError, "running"):
                self.publish()
        def launch_during_replacement(*args, **kwargs):
            return running if not (self.dist / APP_NAME).exists() else ""
        with patch("dist_outputs.subprocess.check_output", side_effect=launch_during_replacement):
            with self.assertRaisesRegex(ValueError, "running"):
                self.publish()
        with patch("dist_outputs.subprocess.check_output", return_value=running):
            self.plugin.write_bytes(b"updated plugin only")
            publish_outputs(self.dist, {PLUGIN_NAME: self.plugin})
        self.assertEqual((self.dist / PLUGIN_NAME).read_bytes(), b"updated plugin only")
        self.assertEqual((self.dist / APP_NAME / "Contents/MacOS/SkyBackcraft").read_text(), "new app")

    def test_unknown_files_are_never_deleted(self):
        sentinel = self.dist / "user-data.sqlite"
        sentinel.write_bytes(b"preserve")
        with self.assertRaisesRegex(ValueError, "Unknown"):
            self.publish()
        self.assertEqual(sentinel.read_bytes(), b"preserve")
        self.assertEqual({p.name for p in self.dist.iterdir()}, {sentinel.name})

    def test_version_output_under_dist_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "directly"):
            validate_output_directory(self.dist / "goal-delivery-v16", canonical_dist=self.dist)
        self.assertEqual(validate_output_directory(self.root / "isolated", canonical_dist=self.dist), self.root / "isolated")

    def test_new_install_cannot_pass_with_a_stale_launchable_dist_copy(self):
        self.publish()
        launch_app = self.dist / APP_NAME
        for relative in ["Contents/MacOS/spot-lab", "Contents/MacOS/SkyBackcraft", "Contents/Info.plist"]:
            with self.subTest(component=relative):
                installed_file = self.app / relative
                previous = installed_file.read_bytes()
                installed_file.write_bytes(b"newer installed build")
                with self.assertRaisesRegex(ValueError, "Stale launchable app"):
                    verify_launch_copies(self.app, [launch_app])
                self.assertEqual((launch_app / relative).read_bytes(), previous)
                installed_file.write_bytes(previous)
        verify_launch_copies(self.app, [launch_app])

    def test_install_gate_requires_complete_install_but_allows_retired_launch_path(self):
        verify_launch_copies(self.app, [self.dist / APP_NAME])
        (self.app / "Contents/MacOS/spot-lab").unlink()
        with self.assertRaisesRegex(ValueError, "Incomplete installed app"):
            verify_launch_copies(self.app, [self.dist / APP_NAME])

    def test_publish_waits_for_exclusive_output_directory_lock(self):
        descriptor = os.open(self.dist, os.O_RDONLY)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        child_code = '''
import fcntl, sys
from pathlib import Path
from dist_outputs import APP_NAME, PLUGIN_NAME, publish_outputs
original = fcntl.flock
def observe_lock(fd, operation):
    print("locking", flush=True)
    original(fd, operation)
fcntl.flock = observe_lock
publish_outputs(Path(sys.argv[1]), {APP_NAME: Path(sys.argv[2]), PLUGIN_NAME: Path(sys.argv[3])})
'''
        child = subprocess.Popen(
            [sys.executable, "-B", "-c", child_code, str(self.dist), str(self.app), str(self.plugin)],
            cwd=Path(__file__).resolve().parents[1], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            self.assertTrue(select.select([child.stdout], [], [], 5)[0], "publisher never reached output locking")
            self.assertEqual(child.stdout.readline().strip(), "locking")
            self.assertIsNone(child.poll())
            self.assertEqual(list(self.dist.iterdir()), [])
        finally:
            os.close(descriptor)
            stdout, stderr = child.communicate(timeout=10)
        self.assertEqual(child.returncode, 0, stdout + stderr)
        self.assertEqual({p.name for p in self.dist.iterdir()}, {APP_NAME, PLUGIN_NAME})


if __name__ == "__main__":
    unittest.main()
