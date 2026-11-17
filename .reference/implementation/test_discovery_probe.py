"""Check source/evidence guards without repeating the source discovery scan."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import discovery_probe as probe


class SourceGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        (self.repo / "source.py").write_text("original source\n")
        self.output = self.root / "evidence"
        self.head = "a" * 40
        self.actual_head = self.head
        self.diff = ""
        self.untracked = ""
        self.status = "?? .reference/"
        self.calls: list[tuple[str, ...]] = []
        self.mock_git = patch.object(probe, "git", side_effect=self.git)
        self.mock_git.start()
        self.addCleanup(self.mock_git.stop)

    def git(self, repo: Path, *args: str) -> str:
        self.assertEqual(repo, self.repo)
        self.calls.append(args)
        if args == ("rev-parse", "HEAD"):
            return self.actual_head
        if args == ("rev-parse", "HEAD^{tree}"):
            return "b" * 40
        if args == ("diff", "--name-only", self.head, "--", ".", ":!.reference"):
            return self.diff
        if args == (
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
            ".",
            ":!.reference",
        ):
            return self.untracked
        if args == ("ls-files", "-z", "--", ".", ":!.reference"):
            return "source.py\0"
        if args == ("status", "--porcelain=v1"):
            return self.status
        self.fail(f"unexpected command: {args}")
        raise AssertionError("unreachable")

    def test_reference_wip_is_recorded_and_unchanged_source_passes(self) -> None:
        before = probe.prepare_output(self.repo, self.output, self.head)
        metadata = {"status": "observed"}
        probe.verify_final_source(self.repo, self.head, before, metadata)
        self.assertEqual(metadata["status"], "observed")
        self.assertEqual(metadata["final_source_verification"], "observed")
        self.assertEqual(before["status"], "?? .reference/")
        self.assertTrue(self.output.is_dir())

    def test_missing_repo_is_refused_before_source_commands(self) -> None:
        with self.assertRaisesRegex(ValueError, "not a directory"):
            probe.prepare_output(self.root / "missing", self.output, self.head)
        self.assertEqual(self.calls, [])
        self.assertFalse(self.output.exists())

    def test_output_inside_product_checkout_is_refused(self) -> None:
        for path in (self.repo, self.repo / "evidence", self.repo / ".reference/evidence"):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "outside"):
                probe.prepare_output(self.repo, path, self.head)
        self.assertEqual(self.calls, [])

    def test_existing_output_is_not_overwritten(self) -> None:
        self.output.mkdir()
        retained = self.output / "original.json"
        retained.write_text("retained evidence")
        with self.assertRaises(FileExistsError):
            probe.prepare_output(self.repo, self.output, self.head)
        self.assertEqual(retained.read_text(), "retained evidence")

    def test_wrong_head_refuses_before_output_creation(self) -> None:
        self.actual_head = "c" * 40
        with self.assertRaisesRegex(ValueError, "exact requested source"):
            probe.prepare_output(self.repo, self.output, self.head)
        self.assertFalse(self.output.exists())

    def test_tracked_product_change_refuses_before_output_creation(self) -> None:
        self.diff = "source.py"
        with self.assertRaisesRegex(ValueError, "product paths differ"):
            probe.prepare_output(self.repo, self.output, self.head)
        self.assertFalse(self.output.exists())

    def test_untracked_product_code_is_refused(self) -> None:
        self.untracked = "new_delegate.py\0"
        with self.assertRaisesRegex(ValueError, "untracked product paths"):
            probe.prepare_output(self.repo, self.output, self.head)
        self.assertFalse(self.output.exists())

    def test_changed_final_head_marks_observation_failed(self) -> None:
        before = probe.checked_source(self.repo, self.head)
        self.actual_head = "c" * 40
        metadata = {"status": "observed"}
        with self.assertRaisesRegex(ValueError, "exact requested source"):
            probe.verify_final_source(self.repo, self.head, before, metadata)
        self.assertEqual(metadata["status"], "failed")
        self.assertEqual(metadata["final_source_verification"], "failed")

    def test_final_tracked_and_untracked_changes_mark_failed(self) -> None:
        before = probe.checked_source(self.repo, self.head)
        for tracked, untracked in (("source.py", ""), ("", "new.py\0")):
            with self.subTest(tracked=tracked, untracked=untracked):
                self.diff, self.untracked = tracked, untracked
                metadata = {"status": "observed"}
                with self.assertRaises(ValueError):
                    probe.verify_final_source(self.repo, self.head, before, metadata)
                self.assertEqual(metadata["status"], "failed")

    def test_actual_changed_bytes_fail_even_if_command_reports_clean(self) -> None:
        before = probe.checked_source(self.repo, self.head)
        (self.repo / "source.py").write_text("changed source\n")
        metadata = {"status": "observed"}
        with self.assertRaisesRegex(ValueError, "inputs changed"):
            probe.verify_final_source(self.repo, self.head, before, metadata)
        self.assertEqual(metadata["status"], "failed")

    def test_deleted_input_cannot_look_like_unchanged_source(self) -> None:
        before = probe.checked_source(self.repo, self.head)
        (self.repo / "source.py").unlink()
        metadata = {"status": "observed"}
        with self.assertRaises(FileNotFoundError):
            probe.verify_final_source(self.repo, self.head, before, metadata)
        self.assertEqual(metadata["status"], "failed")


if __name__ == "__main__":
    unittest.main()
