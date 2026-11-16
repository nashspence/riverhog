"""Real SQLite WAL extraction, consistency and bounded-index reference checks."""

from __future__ import annotations

import sqlite3
import struct
import tempfile
import unittest
from pathlib import Path

from wal_capture import Capture, CaptureRefused, _Index, _scan_wal, capture_pages


def recompute_checksums(data: bytes, *, big_endian: bool = False) -> bytes:
    """Construct adversarial but checksum-valid WAL fixtures independently."""
    result = bytearray(data)
    struct.pack_into(">I", result, 0, 0x377F0683 if big_endian else 0x377F0682)
    page_size = int.from_bytes(result[8:12], "big")
    first = second = 0

    def add(part: bytes) -> None:
        nonlocal first, second
        order = "big" if big_endian else "little"
        for offset in range(0, len(part), 8):
            first = (first + int.from_bytes(part[offset : offset + 4], order) + second) % (1 << 32)
            second = (second + int.from_bytes(part[offset + 4 : offset + 8], order) + first) % (
                1 << 32
            )

    add(result[:24])
    struct.pack_into(">II", result, 24, first, second)
    for offset in range(32, len(result), 24 + page_size):
        add(result[offset : offset + 8])
        add(result[offset + 24 : offset + 24 + page_size])
        struct.pack_into(">II", result, offset + 16, first, second)
    return bytes(result)


class WalCaptureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.path = self.directory / "scratch.sqlite3"
        self.wal = Path(str(self.path) + "-wal")
        self.spool = self.directory / "dirty-index.sqlite3"
        self.db = sqlite3.connect(self.path, isolation_level=None)
        self.db.execute("PRAGMA page_size=512")
        self.db.execute("PRAGMA auto_vacuum=INCREMENTAL")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("PRAGMA wal_autocheckpoint=0")
        self.db.execute("PRAGMA cache_size=-32")
        self.db.execute("CREATE TABLE records(id INTEGER PRIMARY KEY, value BLOB NOT NULL)")
        self.db.execute("CREATE INDEX values_index ON records(value)")

    def tearDown(self) -> None:
        self.db.close()
        self.temp.cleanup()

    def insert(self, count: int, *, start: int = 0, width: int = 200) -> None:
        self.db.execute("BEGIN")
        self.db.executemany(
            "INSERT INTO records VALUES(?,?)",
            ((number, bytes([number % 251]) * width) for number in range(start, start + count)),
        )
        self.db.commit()

    def initial(self) -> tuple[dict[int, bytes | None], int]:
        with capture_pages(
            self.db, self.path, self.spool, initial=True, previous_page_count=0
        ) as capture:
            pages = dict(capture.pages)
            self.assertEqual(capture.database_bytes_read, capture.page_count * capture.page_size)
            self.assertEqual(capture.wal_bytes_read, 0)
            self.assertEqual(capture.copied_page_bytes, capture.database_bytes_read)
        return pages, capture.page_count

    def incremental(self, previous: int, **limits: int) -> tuple[dict[int, bytes | None], int]:
        length = self.wal.stat().st_size
        with capture_pages(
            self.db, self.path, self.spool, initial=False, previous_page_count=previous, **limits
        ) as capture:
            pages = dict(capture.pages)
            self.assertEqual(capture.database_bytes_read, 0)
            self.assertEqual(capture.wal_bytes_read, length + capture.copied_page_bytes)
        self.assertFalse(self.spool.exists())
        return pages, capture.page_count

    def assert_image(self, pages: dict[int, bytes | None], count: int) -> None:
        self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.assertEqual(set(pages), set(range(1, count + 1)))
        self.assertEqual(
            b"".join(pages[number] for number in range(1, count + 1)), self.path.read_bytes()
        )

    @staticmethod
    def apply(pages: dict[int, bytes | None], changes: dict[int, bytes | None]) -> None:
        for number, image in changes.items():
            if image is None:
                pages.pop(number, None)
            else:
                pages[number] = image

    def test_initial_materializes_full_database_exactly_once(self) -> None:
        self.insert(20)
        pages, count = self.initial()
        self.assert_image(pages, count)
        self.assertFalse(self.spool.exists())
        self.assertEqual(self.wal.stat().st_size, 0)

    def test_repeated_page_updates_coalesce_and_match_live_sqlite(self) -> None:
        self.insert(100)
        pages, count = self.initial()
        for value in (b"one", b"two", b"final"):
            self.db.execute("UPDATE records SET value=? WHERE id=40", (value,))
        changes, new_count = self.incremental(count)
        self.assertLess(len(changes), count)
        self.apply(pages, changes)
        self.assert_image(pages, new_count)

    def test_schema_and_index_changes_are_captured_without_sql_interception(self) -> None:
        self.insert(20)
        pages, count = self.initial()
        self.db.execute("ALTER TABLE records ADD COLUMN label TEXT")
        self.db.execute("CREATE INDEX labels_index ON records(label)")
        self.db.execute("UPDATE records SET label='updated' WHERE id=2")
        self.db.execute("DROP INDEX values_index")
        changes, new_count = self.incremental(count)
        self.apply(pages, changes)
        self.assert_image(pages, new_count)

    def test_new_pages_are_captured_and_restored(self) -> None:
        self.insert(3)
        pages, count = self.initial()
        self.insert(100, start=10)
        changes, new_count = self.incremental(count)
        self.assertGreater(new_count, count)
        self.apply(pages, changes)
        self.assert_image(pages, new_count)

    def test_shrink_emits_removals_and_preserves_exact_remaining_pages(self) -> None:
        self.insert(100)
        pages, count = self.initial()
        self.db.execute("DELETE FROM records WHERE id>=5")
        self.db.execute("VACUUM")
        changes, new_count = self.incremental(count)
        self.assertLess(new_count, count)
        self.assertEqual(
            {number for number, image in changes.items() if image is None},
            set(range(new_count + 1, count + 1)),
        )
        self.apply(pages, changes)
        self.assert_image(pages, new_count)

    def test_shrink_and_regrow_in_one_interval_never_restore_removed_images(self) -> None:
        self.insert(100)
        pages, count = self.initial()
        self.db.execute("DELETE FROM records")
        self.db.execute("VACUUM")
        self.insert(40, start=200)
        changes, new_count = self.incremental(count)
        self.apply(pages, changes)
        self.assert_image(pages, new_count)

    def test_zero_length_wal_has_no_incremental_changes(self) -> None:
        self.insert(3)
        _, count = self.initial()
        changes, new_count = self.incremental(count)
        self.assertEqual(changes, {})
        self.assertEqual(new_count, count)

    def test_growth_does_not_force_a_full_database_scan_each_capture(self) -> None:
        self.insert(600)
        _, count = self.initial()
        self.db.execute("UPDATE records SET value=? WHERE id=300", (b"z" * 200,))
        changes, _ = self.incremental(count)
        self.assertLess(len(changes), 20)
        self.assertGreater(count, 300)

    def test_wal_and_distinct_dirty_page_resource_envelopes_refuse_capture(self) -> None:
        self.insert(10)
        original_length = self.wal.stat().st_size
        with self.assertRaises(CaptureRefused):
            with capture_pages(
                self.db,
                self.path,
                self.spool,
                initial=True,
                previous_page_count=0,
                max_wal_bytes=1,
            ):
                pass
        self.assertEqual(self.wal.stat().st_size, original_length)
        _, count = self.initial()
        self.db.execute("UPDATE records SET value=?", (b"x" * 200,))
        for limits in ({"max_wal_bytes": 1}, {"max_spool_pages": 1}):
            with self.subTest(limits=limits), self.assertRaises(CaptureRefused):
                self.incremental(count, **limits)
            self.assertFalse(self.spool.exists())
        changes, _ = self.incremental(count)
        self.assertTrue(changes)

    def test_invalid_or_uncommitted_wal_tail_is_refused_without_guessing(self) -> None:
        self.insert(3)
        _, count = self.initial()
        self.db.execute("UPDATE records SET value=? WHERE id=1", (b"replacement",))
        original = self.wal.read_bytes()
        variants: list[bytes] = [original + b"x", original[:-1]]
        for offset in (0, 4, 24, 32 + 8, 32 + 24):
            bad = bytearray(original)
            bad[offset] ^= 1
            variants.append(bytes(bad))
        last = len(original) - (24 + 512)
        uncommitted = bytearray(original)
        struct.pack_into(">I", uncommitted, last + 4, 0)
        variants.append(recompute_checksums(uncommitted))
        wrong_extent = bytearray(original)
        struct.pack_into(">I", wrong_extent, last + 4, count + 1)
        variants.append(recompute_checksums(wrong_extent))
        try:
            for number, data in enumerate(variants):
                self.wal.write_bytes(data)
                with self.subTest(number=number), self.assertRaises(CaptureRefused):
                    self.incremental(count)
                self.assertFalse(self.spool.exists())
        finally:
            self.wal.write_bytes(original)

    def test_big_endian_wal_checksum_encoding_is_validated(self) -> None:
        self.insert(3)
        _, count = self.initial()
        self.db.execute("UPDATE records SET value=? WHERE id=1", (b"replacement",))
        original = self.wal.read_bytes()
        try:
            self.wal.write_bytes(recompute_checksums(original, big_endian=True))
            changes, _ = self.incremental(count)
            self.assertTrue(changes)
        finally:
            self.wal.write_bytes(original)

    def test_reserved_lock_page_crossing_gets_one_explicit_zero_image(self) -> None:
        # Exercise the actual file-format exception without allocating or
        # scanning a one-gigabyte fixture. Ordinary missing pages still fail.
        page_size = 65536
        lock_page = (0x40000000 // page_size) + 1
        header = struct.pack(">8I", 0x377F0682, 3007000, page_size, 0, 123, 456, 0, 0)
        frame = struct.pack(">6I", lock_page + 1, lock_page + 1, 123, 456, 0, 0)
        data = recompute_checksums(header + frame + bytes(page_size))
        fixture = self.directory / "format-fixture-wal"
        fixture.write_bytes(data)
        capture = Capture(page_size, lock_page + 1, False)
        index = _Index(self.spool, 2, capture)
        try:
            with fixture.open("rb") as handle:
                _scan_wal(handle, index, capture, lock_page - 1, len(data))
            self.assertEqual(
                index.db.execute(
                    "SELECT frame_offset FROM pages WHERE page_no=?", (lock_page,)
                ).fetchone(),
                (-1,),
            )
            self.assertEqual(index.count, 2)
        finally:
            index.close()

    def test_ordinary_missing_new_page_is_refused(self) -> None:
        header = struct.pack(">8I", 0x377F0682, 3007000, 512, 0, 123, 456, 0, 0)
        frame = struct.pack(">6I", 12, 12, 123, 456, 0, 0)
        fixture = self.directory / "missing-page-wal"
        fixture.write_bytes(recompute_checksums(header + frame + bytes(512)))
        capture = Capture(512, 12, False)
        index = _Index(self.spool, 2, capture)
        try:
            with fixture.open("rb") as handle, self.assertRaises(CaptureRefused):
                _scan_wal(handle, index, capture, 10, None)
        finally:
            index.close()

    def test_active_transaction_and_wrong_database_are_refused(self) -> None:
        self.insert(3)
        _, count = self.initial()
        self.db.execute("BEGIN")
        try:
            with self.assertRaises(CaptureRefused):
                self.incremental(count)
        finally:
            self.db.rollback()
        with self.assertRaises(CaptureRefused):
            with capture_pages(
                self.db,
                self.directory / "not-the-owned-database",
                self.spool,
                initial=False,
                previous_page_count=count,
            ):
                pass

    def test_attached_and_temporary_semantic_state_are_refused(self) -> None:
        self.insert(3)
        _, count = self.initial()
        self.db.execute("ATTACH DATABASE ':memory:' AS child")
        with self.assertRaises(CaptureRefused):
            self.incremental(count)
        self.db.execute("DETACH DATABASE child")
        self.db.execute("CREATE TEMP TABLE unsafe_state(value INTEGER)")
        with self.assertRaises(CaptureRefused):
            self.incremental(count)

    def test_owner_mutation_during_capture_is_detected(self) -> None:
        self.insert(10)
        _, count = self.initial()
        self.db.execute("UPDATE records SET value=? WHERE id=1", (b"replacement",))
        with self.assertRaises(CaptureRefused):
            with capture_pages(
                self.db, self.path, self.spool, initial=False, previous_page_count=count
            ) as capture:
                list(capture.pages)
                self.db.execute("UPDATE records SET value=? WHERE id=2", (b"too late",))
        self.assertFalse(self.spool.exists())

    def test_partial_initial_or_incremental_consumption_cannot_complete_capture(self) -> None:
        self.insert(20)
        with self.assertRaises(CaptureRefused):
            with capture_pages(
                self.db, self.path, self.spool, initial=True, previous_page_count=0
            ) as capture:
                next(capture.pages)
        self.assertFalse(capture.exhausted)
        # The test deliberately starts a new isolated initial capture; the live
        # policy disables promotion after an unresumable first-capture failure.
        _, count = self.initial()
        self.db.execute("UPDATE records SET value=?", (b"changed",))
        with self.assertRaises(CaptureRefused):
            with capture_pages(
                self.db, self.path, self.spool, initial=False, previous_page_count=count
            ) as capture:
                next(capture.pages)
        self.assertFalse(capture.exhausted)
        self.assertFalse(self.spool.exists())

    def test_spool_never_overwrites_a_preexisting_resource(self) -> None:
        _, count = self.initial()
        self.spool.write_bytes(b"other owner")
        with self.assertRaises(FileExistsError):
            self.incremental(count)
        self.assertEqual(self.spool.read_bytes(), b"other owner")
        self.spool.unlink()
        sidecar = Path(str(self.spool) + "-journal")
        sidecar.write_bytes(b"other sidecar owner")
        with self.assertRaises(FileExistsError):
            self.incremental(count)
        self.assertEqual(sidecar.read_bytes(), b"other sidecar owner")
        self.assertFalse(self.spool.exists())

    def test_strict_parameter_types_and_positive_limits(self) -> None:
        for arguments in (
            {"initial": 1, "previous_page_count": 0},
            {"initial": True, "previous_page_count": False},
            {"initial": False, "previous_page_count": 0},
            {"initial": True, "previous_page_count": 1},
            {"initial": True, "previous_page_count": 0, "max_wal_bytes": True},
            {"initial": True, "previous_page_count": 0, "max_spool_pages": 0},
        ):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                with capture_pages(self.db, self.path, self.spool, **arguments):
                    pass


if __name__ == "__main__":
    unittest.main()
