"""Real SQLite/filesystem tests of the isolated immutable-generation reference."""

from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from generation_store import Descriptor, GenerationStore, InvalidGeneration, LineageRefused

BINDING = {
    "construction": "exact-original-construction",
    "source_sha256": hashlib.sha256(b"captured-source").hexdigest(),
    "history_sha256": hashlib.sha256(b"history-root").hexdigest(),
    "catalog_sha256": hashlib.sha256(b"catalog").hexdigest(),
    "algorithm": "reference-semantic-state/v1",
    "scope": "journal",
}
PAGE_SIZE = 512


def page(value: int) -> bytes:
    return value.to_bytes(8, "big") * (PAGE_SIZE // 8)


class GenerationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.path = self.directory / "generations.sqlite3"
        self.store = GenerationStore(self.path, create=True)

    def tearDown(self) -> None:
        self.store.close()
        self.temp.cleanup()

    def first(self, count: int = 32) -> Descriptor:
        generation = self.store.begin(base=0, page_size=PAGE_SIZE, binding=BINDING)
        self.store.stage(generation, ((i, page(i)) for i in range(1, count + 1)), batch_pages=8)
        descriptor = self.store.complete(
            generation, page_count=count, control={"accepted_frames": 3, "cursor": "exact-prefix"}
        )
        self.store.selected(descriptor, BINDING)
        return descriptor

    def second(self, first: Descriptor, *, number: int = 2) -> Descriptor:
        generation = self.store.begin(base=first.generation, page_size=PAGE_SIZE, binding=BINDING)
        self.store.stage(generation, [(number, page(1000))])
        count = self.store.manifest(first, BINDING)["page_count"]
        return self.store.complete(generation, page_count=count, control={"accepted_frames": 4})

    def restored(self, descriptor: Descriptor, name: str) -> bytes:
        destination = self.directory / name
        self.store.restore(descriptor, binding=BINDING, destination=destination)
        return destination.read_bytes()

    def drain(self) -> None:
        while self.store.db.execute("SELECT 1 FROM garbage LIMIT 1").fetchone():
            self.store.collect(limit=11)

    def test_initial_restore_includes_bound_control_and_every_page(self) -> None:
        descriptor = self.first()
        target = self.directory / "restored.sqlite3"
        control = self.store.restore(descriptor, binding=BINDING, destination=target)
        self.assertEqual(control, {"accepted_frames": 3, "cursor": "exact-prefix"})
        self.assertEqual(target.read_bytes(), b"".join(page(i) for i in range(1, 33)))

    def test_changed_page_copies_only_eight_paths_and_preserves_predecessor(self) -> None:
        first = self.first(256)
        pages = self.store.counters.page_objects_created
        nodes = self.store.counters.node_objects_created
        second = self.second(first)
        self.assertEqual(self.store.counters.page_objects_created - pages, 1)
        self.assertLessEqual(self.store.counters.node_objects_created - nodes, 8)
        self.store.selected(second, BINDING)
        self.assertEqual(self.restored(first, "old")[PAGE_SIZE : PAGE_SIZE * 2], page(2))
        self.assertEqual(self.restored(second, "new")[PAGE_SIZE : PAGE_SIZE * 2], page(1000))

    def test_initial_and_update_node_creation_has_fixed_depth_bound(self) -> None:
        first = self.first(513)
        self.assertLessEqual(self.store.counters.node_objects_created, 513 * 8)
        for value in range(5):
            generation = self.store.begin(
                base=first.generation, page_size=PAGE_SIZE, binding=BINDING
            )
            before = self.store.counters.node_objects_created
            self.store.stage(generation, [(i, page(value + 2000)) for i in range(10, 20)])
            self.assertLessEqual(self.store.counters.node_objects_created - before, 10 * 8)
            descriptor = self.store.complete(generation, page_count=513, control={})
            self.store.selected(descriptor, BINDING)
            self.store.discard(first.generation)
            first = descriptor
            self.drain()

    def test_further_live_changes_cannot_modify_previous_generations(self) -> None:
        first = self.first()
        second = self.second(first)
        self.store.selected(second, BINDING)
        generation = self.store.begin(base=second.generation, page_size=PAGE_SIZE, binding=BINDING)
        self.store.stage(generation, [(2, page(9999))])
        self.assertEqual(self.restored(second, "second")[PAGE_SIZE : PAGE_SIZE * 2], page(1000))
        self.assertEqual(self.restored(first, "first")[PAGE_SIZE : PAGE_SIZE * 2], page(2))

    def test_complete_refuses_cursor_beyond_missing_page_extent(self) -> None:
        generation = self.store.begin(base=0, page_size=PAGE_SIZE, binding=BINDING)
        self.store.stage(generation, [(1, page(1)), (3, page(3))])
        with self.assertRaises(InvalidGeneration):
            self.store.complete(generation, page_count=3, control={"cursor": 3})

    def test_removed_pages_and_regrowth_do_not_resurrect_old_images(self) -> None:
        first = self.first(32)
        generation = self.store.begin(base=first.generation, page_size=PAGE_SIZE, binding=BINDING)
        self.store.stage(generation, ((i, None) for i in range(17, 33)))
        second = self.store.complete(generation, page_count=16, control={})
        self.store.selected(second, BINDING)
        generation = self.store.begin(base=second.generation, page_size=PAGE_SIZE, binding=BINDING)
        self.store.stage(generation, ((i, page(i + 100)) for i in range(17, 25)))
        third = self.store.complete(generation, page_count=24, control={})
        self.assertEqual(
            self.restored(third, "third")[16 * PAGE_SIZE :],
            b"".join(page(i + 100) for i in range(17, 25)),
        )
        self.assertEqual(len(self.restored(first, "first")), 32 * PAGE_SIZE)

    def test_wrong_source_history_catalog_algorithm_or_construction_refused(self) -> None:
        descriptor = self.first()
        for key in BINDING:
            with self.subTest(key=key), self.assertRaises(InvalidGeneration):
                self.store.restore(
                    descriptor,
                    binding={**BINDING, key: "changed"},
                    destination=self.directory / key,
                )

    def test_foreign_store_and_forged_manifest_refused(self) -> None:
        descriptor = self.first()
        for changed in [
            Descriptor("foreign", descriptor.generation, descriptor.manifest_sha256),
            Descriptor(descriptor.store_id, descriptor.generation, "0" * 64),
        ]:
            with self.assertRaises(InvalidGeneration):
                self.restored(changed, "forged")

    def test_missing_new_page_is_detected_instead_of_substituting_older_page(self) -> None:
        first = self.first()
        second = self.second(first)
        self.store.selected(second, BINDING)
        self.store.db.execute("DELETE FROM objects WHERE kind='page' AND body=?", (page(1000),))
        with self.assertRaises(InvalidGeneration):
            self.restored(second, "corrupt")
        self.assertFalse((self.directory / "corrupt").exists())
        self.assertEqual(self.restored(first, "fallback")[PAGE_SIZE : PAGE_SIZE * 2], page(2))

    def test_corrupt_new_page_falls_back_without_swallowing_source_binding_error(self) -> None:
        first = self.first()
        second = self.second(first)
        self.store.db.execute(
            "UPDATE objects SET body=? WHERE kind='page' AND body=?",
            (
                page(2000),
                page(1000),
            ),
        )
        with self.assertRaises(InvalidGeneration):
            self.restored(second, "corrupt")
        self.assertEqual(len(self.restored(first, "fallback")), 32 * PAGE_SIZE)
        with self.assertRaises(InvalidGeneration):
            self.store.manifest(first, {**BINDING, "source_sha256": "wrong"})

    def test_shared_corruption_invalidates_both_and_requires_authoritative_replay(self) -> None:
        first = self.first()
        second = self.second(first)
        self.store.db.execute("DELETE FROM objects WHERE kind='page' AND body=?", (page(1),))
        for descriptor in [second, first]:
            with self.assertRaises(InvalidGeneration):
                self.restored(descriptor, "bad")

    def test_new_candidate_waits_for_positive_selection_receiving(self) -> None:
        first = self.first()
        second = self.second(first)
        with self.assertRaises(LineageRefused):
            self.store.begin(base=first.generation, page_size=PAGE_SIZE, binding=BINDING)
        self.store.selected(second, BINDING)
        self.store.selected(second, BINDING)
        self.store.begin(base=second.generation, page_size=PAGE_SIZE, binding=BINDING)

    def test_fallback_never_inherits_pages_from_newer_lineage(self) -> None:
        first = self.first()
        second = self.second(first)
        self.store.selected(second, BINDING)
        with self.assertRaises(LineageRefused):
            self.store.begin(base=first.generation, page_size=PAGE_SIZE, binding=BINDING)
        with self.assertRaises(LineageRefused):
            self.store.selected(first, BINDING)

    def test_unselected_candidate_abort_keeps_predecessor_and_allows_new_attempt(self) -> None:
        first = self.first()
        second = self.second(first)
        with self.assertRaises(InvalidGeneration):
            self.store.discard(second.generation)
        self.store.discard(second.generation, rejected_candidate=True)
        self.drain()
        self.assertEqual(self.restored(first, "first")[PAGE_SIZE : PAGE_SIZE * 2], page(2))
        self.store.begin(base=first.generation, page_size=PAGE_SIZE, binding=BINDING)

    def test_reader_pin_blocks_retirement_until_original_receipt(self) -> None:
        first = self.first()
        token = self.store.pin(first, BINDING)
        second = self.second(first)
        self.store.selected(second, BINDING)
        with self.assertRaises(InvalidGeneration):
            self.store.discard(first.generation)
        with self.assertRaises(InvalidGeneration):
            self.store.receive_pin("different-token")
        self.store.receive_pin(token)
        self.store.discard(first.generation)
        self.drain()
        self.assertEqual(self.restored(second, "second")[PAGE_SIZE : PAGE_SIZE * 2], page(1000))

    def test_selected_tip_cannot_be_retired(self) -> None:
        first = self.first()
        with self.assertRaises(InvalidGeneration):
            self.store.discard(first.generation)

    def test_collection_is_bounded_and_does_not_rescan_live_generation(self) -> None:
        first = self.first(128)
        second = self.second(first)
        self.store.selected(second, BINDING)
        self.drain()
        before = self.store.counters.objects_collected
        self.store.discard(first.generation)
        self.assertLessEqual(self.store.collect(limit=3), 3)
        self.drain()
        self.assertLessEqual(self.store.counters.objects_collected - before, 9)
        self.assertEqual(len(self.restored(second, "second")), 128 * PAGE_SIZE)

    def test_no_replace_of_existing_restore_destination(self) -> None:
        first = self.first()
        destination = self.directory / "occupied"
        destination.write_bytes(b"original")
        with self.assertRaises(FileExistsError):
            self.store.restore(first, binding=BINDING, destination=destination)
        self.assertEqual(destination.read_bytes(), b"original")

    def test_partial_candidate_survives_process_exit_without_damaging_selected_root(self) -> None:
        first = self.first()
        script = (
            "import json,os,sys; from pathlib import Path; "
            "from generation_store import GenerationStore; "
            "s=GenerationStore(Path(sys.argv[1])); "
            "g=s.begin(base=s.tip,page_size=512,binding=json.loads(sys.argv[2])); "
            "s.stage(g,[(2,b'x'*512)]); os._exit(23)"
        )
        import json

        result = subprocess.run(
            [sys.executable, "-c", script, str(self.path), json.dumps(BINDING)],
            cwd=Path(__file__).parent,
            check=False,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 23, result.stderr.decode())
        self.assertEqual(self.restored(first, "survivor")[PAGE_SIZE : PAGE_SIZE * 2], page(2))
        with self.assertRaises(LineageRefused):
            self.store.begin(base=first.generation, page_size=PAGE_SIZE, binding=BINDING)

    def test_actual_sqlite_database_can_be_reconstructed_and_independently_queried(self) -> None:
        source = self.directory / "source.sqlite3"
        db = sqlite3.connect(source)
        db.execute("PRAGMA page_size=512")
        db.execute("CREATE TABLE evidence(id INTEGER PRIMARY KEY, value TEXT)")
        db.executemany("INSERT INTO evidence VALUES(?,?)", ((i, f"value-{i}") for i in range(70)))
        db.commit()
        expected = db.execute("SELECT * FROM evidence ORDER BY id").fetchall()
        db.close()
        images = source.read_bytes()
        generation = self.store.begin(base=0, page_size=512, binding=BINDING)
        self.store.stage(
            generation, ((i // 512 + 1, images[i : i + 512]) for i in range(0, len(images), 512))
        )
        descriptor = self.store.complete(generation, page_count=len(images) // 512, control={})
        target = self.directory / "restored.sqlite3"
        self.store.restore(descriptor, binding=BINDING, destination=target)
        restored = sqlite3.connect(target)
        try:
            self.assertEqual(restored.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            self.assertEqual(
                restored.execute("SELECT * FROM evidence ORDER BY id").fetchall(), expected
            )
        finally:
            restored.close()


if __name__ == "__main__":
    unittest.main()
