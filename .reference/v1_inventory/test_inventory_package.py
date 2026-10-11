"""Tests of reference-package integrity, not Riverhog runtime growth."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import tempfile
import unittest

from load_inventory import load_inventory

ROOT = Path(__file__).resolve().parent


class InventoryPackageTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.directory = Path(self.scratch.name)
        for path in ROOT.glob("*.json"):
            shutil.copyfile(path, self.directory / path.name)

    def edit(self, name, change):
        path = self.directory / name
        data = json.loads(path.read_text(encoding="utf-8"))
        change(data)
        path.write_text(json.dumps(data), encoding="utf-8")

    def test_complete_identity_and_counts(self):
        data = load_inventory(self.directory)
        self.assertEqual(len(data["families"]), 64)
        self.assertEqual(len(data["distribution_coverage"]), 86)
        self.assertEqual(sum(len(f["source_anchors"]) for f in data["families"]), 182)
        self.assertEqual(data["audited_source"]["sha"], "152d095038df0c0d0ed642cf265d47b11c65a754")

    def test_public_internal_and_unqualified_status(self):
        data = load_inventory(self.directory)
        self.assertEqual([f["id"] for f in data["families"] if f["layer"] == "internal"], ["G63", "G64"])
        self.assertEqual(len([f for f in data["families"] if f["layer"] == "public"]), 62)
        self.assertTrue(all(f["conformance"] == "not-established-by-this-inventory" for f in data["families"]))
        self.assertFalse(data["verification"]["verification_execution"]["stdout"]["runtime_complexity_tested"])

    def test_pre_v1_hard_cut_and_derived_identity_approval(self):
        data = load_inventory(self.directory)
        g08 = next(f for f in data["families"] if f["id"] == "G08")
        g26 = next(f for f in data["families"] if f["id"] == "G26")
        self.assertIn("hard cuts", g08["title"])
        self.assertIn("Preserve forward-only Alembic", g08["semantic_requirement"])
        self.assertIn("synthetic later-revision", g08["semantic_requirement"])
        self.assertIn("obsolete pre-v1 migration", g08["semantic_requirement"])
        self.assertIn("specifically approved", g26["semantic_requirement"])
        self.assertIn("retaining forward-only revision machinery", g26["semantic_requirement"])

    def test_pre_v1_hard_cut_semantics_have_digest_protection(self):
        def undo_hard_cut(rows):
            next(f for f in rows if f["id"] == "G08")["semantic_requirement"] = "Support old schema migration"
        self.edit("families-G01-G08.json", undo_hard_cut)
        with self.assertRaisesRegex(ValueError, "differs from the reviewed"):
            load_inventory(self.directory)

    def test_alembic_retention_semantics_have_digest_protection(self):
        def erase_framework(rows):
            next(f for f in rows if f["id"] == "G08")["semantic_requirement"] = (
                "Remove forward-only Alembic and all future upgrade tests"
            )
        self.edit("families-G01-G08.json", erase_framework)
        with self.assertRaisesRegex(ValueError, "differs from the reviewed"):
            load_inventory(self.directory)

    def test_changed_bound_is_rejected(self):
        self.edit("families-G01-G08.json", lambda rows: rows[0].update(proposed_work_bound="O(1)"))
        with self.assertRaisesRegex(ValueError, "differs from the reviewed"):
            load_inventory(self.directory)

    def test_missing_part_is_rejected(self):
        (self.directory / "families-G01-G08.json").unlink()
        with self.assertRaises(FileNotFoundError):
            load_inventory(self.directory)

    def test_conformance_override_is_rejected(self):
        self.edit("families-G01-G08.json", lambda rows: rows[0].update(conformance="passed"))
        with self.assertRaisesRegex(ValueError, "overrides shared"):
            load_inventory(self.directory)

    def test_outside_part_path_is_rejected(self):
        self.edit("metadata.json", lambda data: data["publication_representation"].update(parts=["../outside.json"]))
        with self.assertRaisesRegex(ValueError, "invalid inventory part path"):
            load_inventory(self.directory)

    def test_source_fingerprint_change_is_rejected(self):
        self.edit("sources.json", lambda data: data.update({next(iter(data)): "0" * 64}))
        with self.assertRaisesRegex(ValueError, "differs from the reviewed"):
            load_inventory(self.directory)


if __name__ == "__main__":
    unittest.main()
