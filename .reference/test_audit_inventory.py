"""Tests of the reference source reader and coverage guard, not product witnesses."""
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("audit_inventory", Path(__file__).with_name("audit_inventory.py"))
assert spec and spec.loader
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class CoverageTests(unittest.TestCase):
    def test_exact(self):
        audit.require_exact_coverage({"observer", "sampler"}, ["sampler", "observer"])

    def test_missing(self):
        with self.assertRaisesRegex(ValueError, "missing"):
            audit.require_exact_coverage({"observer", "sampler"}, ["observer"])

    def test_stale(self):
        with self.assertRaisesRegex(ValueError, "stale"):
            audit.require_exact_coverage({"observer"}, ["observer", "retired"])

    def test_duplicate(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            audit.require_exact_coverage({"observer"}, ["observer", "observer"])

    def test_empty(self):
        with self.assertRaisesRegex(ValueError, "empty"):
            audit.require_exact_coverage(set(), [])

    def test_added_subject_demands_coverage(self):
        covered = ["observer"]
        audit.require_exact_coverage(set(covered), covered)
        with self.assertRaises(ValueError):
            audit.require_exact_coverage({"observer", "new-sampler"}, covered)


class SourceTests(unittest.TestCase):
    def facts(self, text):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "app.py"
            path.write_text(text, encoding="utf-8")
            return audit.source_facts(path, root)

    def test_constant_prefix_is_resolved(self):
        facts = self.facts("import os\nPREFIX='ROLE'\nx=os.getenv(f'{PREFIX}_STATE_ROOT')\n")
        self.assertEqual(facts["environment"][0]["name"], "ROLE_STATE_ROOT")

    def test_dynamic_name_stays_unresolved(self):
        facts = self.facts("import os\nx=os.getenv(runtime_name())\n")
        self.assertIsNone(facts["environment"][0]["name"])

    def test_conflicting_prefix_stays_unresolved(self):
        facts = self.facts("import os\nPREFIX='ONE'\nPREFIX='TWO'\nx=os.getenv(f'{PREFIX}_STATE')\n")
        self.assertIsNone(facts["environment"][0]["name"])

    def test_option_records_own_line(self):
        facts = self.facts("parser.add_argument('--execution-lease-file')\n")
        self.assertEqual(facts["arguments"][0]["line"], 1)
        self.assertEqual(facts["arguments"][0]["names"], ["--execution-lease-file"])

    def test_formatted_prefix_not_guessed(self):
        facts = self.facts("import os\nPREFIX='ROLE'\nx=os.getenv(f'{PREFIX!r}_STATE')\n")
        self.assertIsNone(facts["environment"][0]["name"])

    def test_input_is_not_executed(self):
        facts = self.facts("raise RuntimeError('must not run')\n")
        self.assertEqual(facts["arguments"], [])

    def test_hash_changes_with_source(self):
        self.assertNotEqual(self.facts("x=1\n")["sha256"], self.facts("x=2\n")["sha256"])


class SnapshotTests(unittest.TestCase):
    def test_supplied_snapshot_is_mechanically_closed(self):
        root = os.environ.get("RIVERHOG_REFERENCE_ROOT")
        if not root:
            self.skipTest("set RIVERHOG_REFERENCE_ROOT to the audited source")
        facts = audit.inventory(Path(root))
        self.assertEqual((facts["project_count"], facts["executable_count"], facts["image_count"]), (38, 20, 10))
        # Snapshot facts only. This assertion is not a claim that parity is implemented.
        lease = [entry["command"] for project in facts["projects"] for entry in project["scripts"]
                 if any("--execution-lease-file" in option["names"] for option in entry["source"]["arguments"])]
        self.assertEqual(lease, ["a-stove0-nvenc-av1-opus-target"])


if __name__ == "__main__":
    unittest.main()
