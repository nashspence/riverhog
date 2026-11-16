"""Compose actual SQLite WAL capture, immutable pages and a separate selector DB.

The tiny selector is a test witness for persistence ordering, not Core's
authorization/accounting implementation. This suite does not certify Native
state closure, production receiving, a policy threshold or power-loss safety.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from generation_store import Descriptor, GenerationStore, InvalidGeneration, LineageRefused
from wal_capture import CaptureRefused, capture_pages

BINDING = {"source": "exact-captured-source", "algorithm": "composition-reference/v1"}


class LostSelectionResponse(RuntimeError):
    pass


class StaleOwner(RuntimeError):
    pass


class CaptureCompositionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.database = self.directory / "working.sqlite3"
        self.db = sqlite3.connect(self.database, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("PRAGMA wal_autocheckpoint=0")
        self.db.execute("CREATE TABLE facts(id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
        self.db.execute("CREATE INDEX facts_body ON facts(body)")
        self.db.execute("BEGIN")
        self.db.executemany(
            "INSERT INTO facts VALUES(?,?)", ((i, f"{i:04d}" + "x" * 900) for i in range(100))
        )
        self.db.commit()
        self.store = GenerationStore(self.directory / "generations.sqlite3", create=True)
        self.owner = sqlite3.connect(self.directory / "owner.sqlite3", isolation_level=None)
        self.owner.execute("PRAGMA synchronous=EXTRA")
        self.owner.execute(
            "CREATE TABLE frontier(singleton INTEGER PRIMARY KEY, epoch INTEGER, "
            "generation INTEGER, digest TEXT, selected_count INTEGER)"
        )
        self.owner.execute("INSERT INTO frontier VALUES(1,1,0,NULL,0)")
        self.last_count = 0

    def tearDown(self) -> None:
        self.owner.close()
        self.store.close()
        self.db.close()
        self.temp.cleanup()

    def rows(self, db: sqlite3.Connection) -> list[tuple[int, str]]:
        return db.execute("SELECT id,body FROM facts ORDER BY id").fetchall()

    def prepare(self, *, base: int) -> tuple[Descriptor, dict[str, int]]:
        with capture_pages(
            self.db,
            self.database,
            self.directory / "capture.sqlite3",
            initial=base == 0,
            previous_page_count=self.last_count,
            max_wal_bytes=4 * 1024 * 1024,
            max_spool_pages=2048,
        ) as capture:
            generation = self.store.begin(base=base, page_size=capture.page_size, binding=BINDING)
            self.store.stage(generation, capture.pages, batch_pages=8)
            page_count = capture.page_count
            counters = {
                "database_bytes_read": capture.database_bytes_read,
                "wal_bytes_read": capture.wal_bytes_read,
                "copied_page_bytes": capture.copied_page_bytes,
                "wal_frames": capture.wal_frames,
            }
        # Successful exhaustive context exit is part of the persistence barrier.
        descriptor = self.store.complete(
            generation, page_count=page_count, control={"generation_extent": page_count}
        )
        return descriptor, counters

    def select(
        self, descriptor: Descriptor, *, epoch: int = 1, lose_response: bool = False
    ) -> None:
        self.store.manifest(descriptor, BINDING)
        self.owner.execute("BEGIN IMMEDIATE")
        changed = self.owner.execute(
            "UPDATE frontier SET generation=?,digest=?,selected_count=selected_count+1 "
            "WHERE singleton=1 AND epoch=?",
            (descriptor.generation, descriptor.manifest_sha256, epoch),
        ).rowcount
        if changed != 1:
            self.owner.rollback()
            raise StaleOwner("the exact owner epoch is no longer current")
        self.owner.commit()
        if lose_response:
            raise LostSelectionResponse("selection committed before reply was lost")
        self.receive_selected(descriptor)

    def receive_selected(self, descriptor: Descriptor) -> None:
        row = self.owner.execute("SELECT generation,digest FROM frontier").fetchone()
        if row != (descriptor.generation, descriptor.manifest_sha256):
            raise InvalidGeneration("external selected descriptor differs")
        self.store.selected(descriptor, BINDING)
        self.last_count = self.store.manifest(descriptor, BINDING)["page_count"]
        self.assertEqual(self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone(), (0, 0, 0))

    def first(self) -> Descriptor:
        descriptor, _ = self.prepare(base=0)
        self.select(descriptor)
        return descriptor

    def restore_rows(self, descriptor: Descriptor, name: str) -> list[tuple[int, str]]:
        destination = self.directory / name
        self.store.restore(descriptor, binding=BINDING, destination=destination)
        db = sqlite3.connect(destination)
        try:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            return self.rows(db)
        finally:
            db.close()

    def test_real_update_roundtrip_only_reads_wal_and_copies_changed_pages(self) -> None:
        initial_rows = self.rows(self.db)
        first = self.first()
        self.db.execute("UPDATE facts SET body=? WHERE id=55", ("replacement" + "y" * 850,))
        second, counters = self.prepare(base=first.generation)
        expected = self.rows(self.db)
        self.select(second)
        self.assertEqual(counters["database_bytes_read"], 0)
        self.assertGreater(counters["wal_bytes_read"], 0)
        self.assertLess(counters["copied_page_bytes"], self.last_count * 4096)
        self.assertEqual(self.restore_rows(first, "first.sqlite3"), initial_rows)
        self.assertEqual(self.restore_rows(second, "second.sqlite3"), expected)

    def test_later_scratch_mutations_do_not_rewrite_selected_images(self) -> None:
        first = self.first()
        self.db.execute("UPDATE facts SET body='second' WHERE id=55")
        second, _ = self.prepare(base=first.generation)
        selected_rows = self.rows(self.db)
        self.select(second)
        self.db.execute("DELETE FROM facts WHERE id<50")
        self.db.execute("INSERT INTO facts VALUES(500,'later')")
        self.assertNotEqual(self.rows(self.db), selected_rows)
        self.assertEqual(self.restore_rows(second, "selected.sqlite3"), selected_rows)

    def test_complete_candidate_before_selector_commit_preserves_old_frontier(self) -> None:
        expected = self.rows(self.db)
        first = self.first()
        self.db.execute("UPDATE facts SET body='candidate' WHERE id=1")
        second, _ = self.prepare(base=first.generation)
        self.assertEqual(
            self.owner.execute("SELECT generation,selected_count FROM frontier").fetchone(),
            (
                first.generation,
                1,
            ),
        )
        with self.assertRaises(LineageRefused):
            self.store.begin(base=first.generation, page_size=4096, binding=BINDING)
        self.store.discard(second.generation, rejected_candidate=True)
        self.assertEqual(self.restore_rows(first, "old.sqlite3"), expected)

    def test_lost_selection_reply_reconciles_exact_descriptor_without_double_count(self) -> None:
        first = self.first()
        self.db.execute("UPDATE facts SET body='selected' WHERE id=1")
        second, _ = self.prepare(base=first.generation)
        expected = self.rows(self.db)
        with self.assertRaises(LostSelectionResponse):
            self.select(second, lose_response=True)
        self.assertEqual(self.store.tip, first.generation)
        self.assertEqual(self.owner.execute("SELECT selected_count FROM frontier").fetchone(), (2,))
        self.receive_selected(second)
        self.receive_selected(second)
        self.assertEqual(self.owner.execute("SELECT selected_count FROM frontier").fetchone(), (2,))
        self.assertEqual(self.restore_rows(second, "accepted.sqlite3"), expected)

    def test_stale_worker_cannot_select_a_ready_candidate(self) -> None:
        first = self.first()
        self.db.execute("UPDATE facts SET body='not-selected' WHERE id=1")
        second, _ = self.prepare(base=first.generation)
        self.owner.execute("UPDATE frontier SET epoch=2")
        with self.assertRaises(StaleOwner):
            self.select(second, epoch=1)
        self.assertEqual(
            self.owner.execute("SELECT generation,selected_count FROM frontier").fetchone(),
            (
                first.generation,
                1,
            ),
        )

    def test_partial_incremental_iterator_cannot_seal_mixed_state(self) -> None:
        first = self.first()
        self.db.execute("UPDATE facts SET body='changed' WHERE id%2=0")
        with self.assertRaises(CaptureRefused):
            with capture_pages(
                self.db,
                self.database,
                self.directory / "capture.sqlite3",
                initial=False,
                previous_page_count=self.last_count,
            ) as capture:
                generation = self.store.begin(
                    base=first.generation, page_size=capture.page_size, binding=BINDING
                )
                self.store.stage(generation, [next(capture.pages)])
        self.assertEqual(
            self.owner.execute("SELECT generation FROM frontier").fetchone(), (first.generation,)
        )

    def test_owner_mutation_at_end_of_capture_refuses_before_envelope_seal(self) -> None:
        first = self.first()
        self.db.execute("UPDATE facts SET body='first-change' WHERE id=1")
        with self.assertRaises(CaptureRefused):
            with capture_pages(
                self.db,
                self.database,
                self.directory / "capture.sqlite3",
                initial=False,
                previous_page_count=self.last_count,
            ) as capture:
                generation = self.store.begin(
                    base=first.generation, page_size=capture.page_size, binding=BINDING
                )
                self.store.stage(generation, capture.pages)
                self.db.execute("UPDATE facts SET body='late-change' WHERE id=2")
        self.assertEqual(
            self.store.db.execute(
                "SELECT status,manifest FROM generations WHERE id=?", (generation,)
            ).fetchone(),
            ("building", None),
        )

    def test_rejected_candidate_can_retry_original_wal_interval_without_new_full_copy(self) -> None:
        first = self.first()
        self.db.execute("UPDATE facts SET body='retry-me' WHERE id=1")
        second, _ = self.prepare(base=first.generation)
        self.store.discard(second.generation, rejected_candidate=True)
        self.db.execute("UPDATE facts SET body='also-new' WHERE id=2")
        third, counters = self.prepare(base=first.generation)
        expected = self.rows(self.db)
        self.select(third)
        self.assertEqual(counters["database_bytes_read"], 0)
        self.assertEqual(self.restore_rows(third, "retried.sqlite3"), expected)


if __name__ == "__main__":
    unittest.main()
