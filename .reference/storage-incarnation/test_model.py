"""Focused #908 regressions; standard-library only, with real persistent files."""

from __future__ import annotations

import hashlib
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from model import (
    LOCK,
    MAGIC,
    MARKER,
    BindingUnavailable,
    Catalog,
    Evidence,
    FilesystemWitness,
    IdentityError,
    IdentityMismatch,
    IdentityUnavailable,
    incarnation,
    provision,
)

HERE = Path(__file__).parent


class Endpoint:
    """A stable URL whose trusted adapter/backend can change without notice."""

    def __init__(self, backend: FilesystemWitness) -> None:
        self.backend = backend
        self.after_describe: FilesystemWitness | None = None
        self.effects: list[tuple[str, str]] = []
        self.online = True
        self.readable = True
        self.writable = True

    def describe(self) -> Evidence:
        if not self.online:
            raise IdentityUnavailable("endpoint unavailable")
        result = self.backend.describe()
        if self.after_describe is not None:
            self.backend, self.after_describe = self.after_describe, None
        return Evidence(result.incarnation_id, self.readable, self.writable)

    def read(self, expected_incarnation: str, object_key: str) -> bytes:
        if not self.online:
            raise IdentityUnavailable("endpoint unavailable")
        result = self.backend.read(expected_incarnation, object_key)
        self.effects.append((expected_incarnation, object_key))
        return result


class IdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "store"
        self.root.mkdir()

    def test_identity_is_stable_across_independent_adapter_processes(self) -> None:
        expected = provision(self.root)
        command = [sys.executable, "-c",
                   "from model import FilesystemWitness; from pathlib import Path; "
                   "import sys; print(FilesystemWitness(Path(sys.argv[1])).describe().incarnation_id)",
                   str(self.root)]
        for _ in range(2):
            result = subprocess.run(command, cwd=HERE, check=True, capture_output=True, text=True)
            self.assertEqual(result.stdout.strip(), expected)

    def test_parallel_provisioners_publish_one_identity(self) -> None:
        command = [sys.executable, "-c",
                   "from model import provision; from pathlib import Path; "
                   "import sys; print(provision(Path(sys.argv[1])))", str(self.root)]
        processes = [subprocess.Popen(command, cwd=HERE, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, text=True) for _ in range(6)]
        identities = []
        try:
            for process in processes:
                stdout, stderr = process.communicate(timeout=15)
                self.assertEqual(process.returncode, 0, stderr)
                identities.append(stdout.strip())
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
        self.assertEqual(len(set(identities)), 1)
        self.assertEqual(identities[0], FilesystemWitness(self.root).describe().incarnation_id)
        self.assertEqual({p.name for p in self.root.iterdir()}, {MARKER, LOCK})

    def test_descriptor_never_bootstraps_missing_identity(self) -> None:
        with self.assertRaises(IdentityUnavailable):
            FilesystemWitness(self.root).describe()
        self.assertEqual(list(self.root.iterdir()), [])

    def test_explicit_provisioning_refuses_unmarked_data(self) -> None:
        (self.root / "old.enc").write_bytes(b"historical ciphertext")
        with self.assertRaises(IdentityError):
            provision(self.root)
        self.assertFalse((self.root / MARKER).exists())
        self.assertEqual((self.root / "old.enc").read_bytes(), b"historical ciphertext")

    def test_reprovision_does_not_change_existing_identity_or_bytes(self) -> None:
        expected = provision(self.root)
        original = (self.root / MARKER).read_bytes()
        (self.root / "old.enc").write_bytes(b"ciphertext")
        self.assertEqual(provision(self.root), expected)
        self.assertEqual((self.root / MARKER).read_bytes(), original)

    def test_corrupt_truncated_unknown_and_noncanonical_markers_fail_closed(self) -> None:
        bad = [b"", b"{", b"unknown/v9\n" + str(uuid4()).encode() + b"\n",
               MAGIC + str(uuid4()).upper().encode() + b"\n",
               MAGIC + b"0" * 36 + b"\n", MAGIC + b"\xff" * 36 + b"\n",
               MAGIC + str(uuid4()).encode() + b"\nextra"]
        for raw in bad:
            with self.subTest(raw=raw):
                (self.root / MARKER).write_bytes(raw)
                with self.assertRaises(IdentityError):
                    FilesystemWitness(self.root).describe()
                with self.assertRaises(IdentityError):
                    provision(self.root)
                self.assertEqual((self.root / MARKER).read_bytes(), raw)

    def test_existing_marker_requires_successful_directory_sync(self) -> None:
        expected = provision(self.root)
        original = (self.root / MARKER).read_bytes()
        with patch("model.os.fsync", side_effect=OSError("directory sync failed")):
            with self.assertRaises(OSError):
                provision(self.root)
        self.assertEqual((self.root / MARKER).read_bytes(), original)
        self.assertEqual(provision(self.root), expected)

    def test_marker_symlink_is_not_accepted(self) -> None:
        target = Path(self.temp.name) / "outside"
        target.write_bytes(MAGIC + str(uuid4()).encode() + b"\n")
        (self.root / MARKER).symlink_to(target)
        with self.assertRaises(IdentityUnavailable):
            FilesystemWitness(self.root).describe()

    def test_same_path_recreated_is_a_different_incarnation(self) -> None:
        old = provision(self.root)
        witness = FilesystemWitness(self.root)
        self.root.rename(self.root.with_name("old-root"))
        self.root.mkdir()
        new = provision(self.root)
        (self.root / "object.enc").write_bytes(b"other ciphertext")
        self.assertNotEqual(old, new)
        with self.assertRaises(IdentityMismatch):
            witness.read(old, "object.enc")

    def test_missing_marker_after_enrollment_is_not_repaired(self) -> None:
        expected = provision(self.root)
        (self.root / "object.enc").write_bytes(b"ciphertext")
        (self.root / MARKER).unlink()
        with self.assertRaises(IdentityUnavailable):
            FilesystemWitness(self.root).read(expected, "object.enc")
        self.assertFalse((self.root / MARKER).exists())

    def test_root_is_pinned_across_identity_check_and_object_io(self) -> None:
        expected = provision(self.root)
        (self.root / "object.enc").write_bytes(b"original ciphertext")
        replacement = self.root.with_name("replacement")
        replacement.mkdir()
        provision(replacement)
        (replacement / "object.enc").write_bytes(b"wrong ciphertext")
        import model
        read_identity = model._read_identity

        def move_after_check(fd: int) -> str:
            result = read_identity(fd)
            self.root.rename(self.root.with_name("old-root"))
            replacement.rename(self.root)
            return result

        with patch("model._read_identity", side_effect=move_after_check):
            self.assertEqual(FilesystemWitness(self.root).read(expected, "object.enc"),
                             b"original ciphertext")

    def test_marker_is_not_part_of_public_object_namespace(self) -> None:
        expected = provision(self.root)
        for key in (MARKER, "../outside", "/absolute", "sub/path", ""):
            with self.subTest(key=key), self.assertRaises(ValueError):
                FilesystemWitness(self.root).read(expected, key)

    def test_uuid_validation_does_not_accept_aliases(self) -> None:
        for value in ("archive", "https://store.example", str(uuid4()).upper(),
                      uuid4().hex, "00000000-0000-0000-0000-000000000000", None):
            with self.subTest(value=value), self.assertRaises(IdentityError):
                incarnation(value)

    def test_fsync_failure_does_not_publish_a_marker(self) -> None:
        with patch("model.os.fsync", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                provision(self.root)
        self.assertFalse((self.root / MARKER).exists())
        with self.assertRaises(IdentityError):
            provision(self.root)  # Crash debris is not mistaken for an empty backend.


class CatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        base = Path(self.temp.name)
        self.a, self.b = base / "a", base / "b"
        self.a.mkdir()
        self.b.mkdir()
        self.owner_a, self.owner_b = provision(self.a), provision(self.b)
        (self.a / "object.enc").write_bytes(b"archive A ciphertext")
        (self.b / "object.enc").write_bytes(b"archive B ciphertext")
        self.endpoint = Endpoint(FilesystemWitness(self.a))
        self.db_path = base / "catalog.sqlite3"
        Catalog.initialize(self.db_path)
        self.catalog = Catalog(self.db_path)
        self.addCleanup(lambda: self.catalog.close())
        self.catalog.bind("archive", "https://adapter.example", self.owner_a, self.endpoint)
        self.catalog.record_copy("archive", "historical", "object.enc",
                                 receipt_incarnation=self.owner_a)
        self.catalog.record_work("archive", "pending", "object.enc",
                                 expected_incarnation=self.owner_a)
        self.original_holdings = self.catalog.holdings()
        self.original_work = self.catalog.work()

    def assert_history_unchanged(self) -> None:
        self.assertEqual(self.catalog.holdings(), self.original_holdings)
        self.assertEqual(self.catalog.work(), self.original_work)

    def test_removal_preserves_ownership_and_pending_work(self) -> None:
        self.catalog.remove("archive")
        self.assert_history_unchanged()
        self.assertEqual(self.catalog.administrative_state(self.owner_a), "disabled")
        with self.assertRaises(BindingUnavailable):
            self.catalog.read_copy("historical")
        self.assertEqual(self.endpoint.effects, [])

    def test_same_store_readded_after_catalog_and_adapter_restart(self) -> None:
        self.catalog.remove("archive")
        self.catalog.close()
        self.catalog = Catalog(self.db_path)
        with self.assertRaises(BindingUnavailable):
            self.catalog.read_copy("historical")
        fresh = Endpoint(FilesystemWitness(self.a))
        self.catalog.bind("archive", "https://adapter.example", self.owner_a, fresh)
        self.assertEqual(self.catalog.read_copy("historical"), b"archive A ciphertext")
        self.assert_history_unchanged()

    def test_catalog_rows_alone_do_not_restore_verified_clients(self) -> None:
        self.catalog.close()
        self.catalog = Catalog(self.db_path)
        self.assertEqual(self.catalog.observations, {})
        self.assertEqual(self.catalog.administrative_state(self.owner_a), "bound")
        with self.assertRaises(BindingUnavailable):
            self.catalog.read_copy("historical")

    def test_same_name_and_url_cannot_prove_a_different_backend(self) -> None:
        name, url = "archive", "https://adapter.example"
        old_hash = hashlib.sha256(f"riverhog-archive-binding/v1\0{name}\0{url}".encode()).hexdigest()
        self.catalog.remove(name)
        self.endpoint.backend = FilesystemWitness(self.b)
        new_hash = hashlib.sha256(f"riverhog-archive-binding/v1\0{name}\0{url}".encode()).hexdigest()
        self.assertEqual(old_hash, new_hash)  # The audited tuple fence cannot distinguish this.
        with self.assertRaises(IdentityMismatch):
            self.catalog.bind(name, url, self.owner_a, self.endpoint)
        with self.assertRaises(BindingUnavailable):
            self.catalog.read_copy("historical")
        self.assert_history_unchanged()
        self.assertEqual(self.endpoint.effects, [])

    def test_changing_expected_id_does_not_implicitly_replace_name(self) -> None:
        self.catalog.remove("archive")
        self.endpoint.backend = FilesystemWitness(self.b)
        with self.assertRaises(IdentityMismatch):
            self.catalog.bind("archive", "https://adapter.example", self.owner_b, self.endpoint)
        self.assert_history_unchanged()

    def test_explicit_name_reuse_keeps_historical_owner_isolated(self) -> None:
        self.catalog.remove("archive")
        replacement = Endpoint(FilesystemWitness(self.b))
        self.catalog.bind("archive", "https://adapter.example", self.owner_b, replacement,
                          replacing=self.owner_a)
        self.assert_history_unchanged()
        with self.assertRaises(BindingUnavailable):
            self.catalog.read_copy("historical")
        self.assertEqual(replacement.effects, [])
        self.catalog.record_copy("archive", "new", "object.enc",
                                 receipt_incarnation=self.owner_b)
        self.assertEqual(self.catalog.read_copy("new"), b"archive B ciphertext")
        self.catalog.bind("old-archive", "https://old.example", self.owner_a,
                          Endpoint(FilesystemWitness(self.a)))
        self.assertEqual(self.catalog.read_copy("historical"), b"archive A ciphertext")
        self.assertEqual(dict((row[0], row[1]) for row in self.catalog.holdings()),
                         {"historical": self.owner_a, "new": self.owner_b})
        self.assertEqual(self.catalog.work(), self.original_work)

    def test_late_receipt_and_work_cannot_take_a_reused_names_new_owner(self) -> None:
        self.catalog.bind("archive", "https://adapter.example", self.owner_b,
                          Endpoint(FilesystemWitness(self.b)), replacing=self.owner_a)
        with self.assertRaises(IdentityMismatch):
            self.catalog.record_copy("archive", "late", "object.enc",
                                     receipt_incarnation=self.owner_a)
        with self.assertRaises(IdentityMismatch):
            self.catalog.record_work("archive", "late", "object.enc",
                                     expected_incarnation=self.owner_a)
        self.assert_history_unchanged()

    def test_same_owner_can_move_to_different_name_and_url(self) -> None:
        self.catalog.remove("archive")
        self.catalog.bind("renamed", "https://different.example", self.owner_a, self.endpoint)
        self.assertEqual(self.catalog.read_copy("historical"), b"archive A ciphertext")
        self.assert_history_unchanged()

    def test_same_name_different_url_same_owner_is_verified(self) -> None:
        self.catalog.bind("archive", "https://different.example", self.owner_a, self.endpoint)
        self.assertEqual(self.catalog.read_copy("historical"), b"archive A ciphertext")
        self.assert_history_unchanged()

    def test_duplicate_active_alias_is_rejected(self) -> None:
        with self.assertRaises(IdentityMismatch):
            self.catalog.bind("alias", "https://alias.example", self.owner_a, self.endpoint)
        self.assertEqual(self.catalog.read_copy("historical"), b"archive A ciphertext")

    def test_failed_rebind_invalidates_prior_live_admission(self) -> None:
        other = Endpoint(FilesystemWitness(self.b))
        with self.assertRaises(IdentityMismatch):
            self.catalog.bind("archive", "https://new.example", self.owner_a, other)
        with self.assertRaises(BindingUnavailable):
            self.catalog.read_copy("historical")
        self.assertEqual(self.catalog.observations["archive"], "mismatch")
        self.assert_history_unchanged()

    def test_repoint_after_startup_is_detected_before_object_io(self) -> None:
        self.endpoint.backend = FilesystemWitness(self.b)
        with self.assertRaises(IdentityMismatch):
            self.catalog.read_copy("historical")
        self.assert_history_unchanged()
        self.assertEqual(self.endpoint.effects, [])

    def test_repoint_between_probe_and_effect_is_also_rejected(self) -> None:
        self.endpoint.after_describe = FilesystemWitness(self.b)
        with self.assertRaises(IdentityMismatch):
            self.catalog.read_copy("historical")
        self.assertEqual(self.endpoint.effects, [])
        self.assert_history_unchanged()
        self.assertEqual(self.catalog.observations["archive"], "mismatch")

    def test_unavailable_store_does_not_reassign_history_or_disable_other_store(self) -> None:
        healthy = Endpoint(FilesystemWitness(self.b))
        self.catalog.bind("healthy", "https://healthy.example", self.owner_b, healthy)
        self.catalog.record_copy("healthy", "healthy-copy", "object.enc",
                                 receipt_incarnation=self.owner_b)
        self.endpoint.online = False
        with self.assertRaises(IdentityUnavailable):
            self.catalog.bind("archive", "https://adapter.example", self.owner_a, self.endpoint)
        self.assertEqual(self.catalog.administrative_state(self.owner_a), "bound")
        self.assertEqual(self.catalog.observations["archive"], "unavailable")
        self.assertEqual(self.catalog.read_copy("healthy-copy"), b"archive B ciphertext")
        self.assertEqual(self.catalog.work(), self.original_work)
        self.assertIn(self.original_holdings[0], self.catalog.holdings())

    def test_readability_and_writability_are_not_identity(self) -> None:
        self.endpoint.writable = False
        self.assertEqual(self.catalog.read_copy("historical"), b"archive A ciphertext")
        with self.assertRaises(BindingUnavailable):
            self.catalog.record_work("archive", "new-work", "object.enc",
                                     expected_incarnation=self.owner_a)
        self.endpoint.readable = False
        with self.assertRaises(BindingUnavailable):
            self.catalog.read_copy("historical")
        self.assertEqual(self.catalog.administrative_state(self.owner_a), "bound")
        self.assert_history_unchanged()

    def test_remove_in_another_catalog_invalidates_local_generation(self) -> None:
        other = Catalog(self.db_path)
        try:
            other.remove("archive")
        finally:
            other.close()
        with self.assertRaises(BindingUnavailable):
            self.catalog.read_copy("historical")
        self.assertEqual(self.endpoint.effects, [])

    def test_concurrent_catalog_change_during_probe_is_not_overwritten(self) -> None:
        other = Catalog(self.db_path)
        self.addCleanup(other.close)
        describe = self.endpoint.describe

        def concurrent_remove() -> Evidence:
            result = describe()
            other.remove("archive")
            return result

        with patch.object(self.endpoint, "describe", side_effect=concurrent_remove):
            with self.assertRaises(BindingUnavailable):
                self.catalog.bind("archive", "https://adapter.example", self.owner_a, self.endpoint)
        self.assertEqual(self.catalog.administrative_state(self.owner_a), "disabled")
        self.assert_history_unchanged()

    def test_wrong_explicit_replacement_cas_does_not_change_history(self) -> None:
        self.catalog.remove("archive")
        with self.assertRaises(IdentityMismatch):
            self.catalog.bind("archive", "https://adapter.example", self.owner_b,
                              Endpoint(FilesystemWitness(self.b)), replacing=str(uuid4()))
        self.assert_history_unchanged()

    def test_database_rejects_historical_owner_rewrites(self) -> None:
        self.catalog.bind("other", "https://other.example", self.owner_b,
                          Endpoint(FilesystemWitness(self.b)))
        for table in ("archive_copies", "pending_work"):
            with self.subTest(table=table), self.assertRaises(sqlite3.IntegrityError):
                self.catalog._db.execute(f"UPDATE {table} SET owner=?", (self.owner_b,))
        self.assert_history_unchanged()

    def test_missing_catalog_is_not_created_by_runtime_open(self) -> None:
        missing = self.db_path.with_name("missing.sqlite3")
        with self.assertRaises(sqlite3.OperationalError):
            Catalog(missing)
        self.assertFalse(missing.exists())

    def test_database_rejects_missing_or_unknown_historical_owner(self) -> None:
        for table in ("archive_copies", "pending_work"):
            for owner in (None, str(uuid4())):
                with self.subTest(table=table, owner=owner):
                    with self.assertRaises(sqlite3.IntegrityError):
                        self.catalog._db.execute(
                            f"INSERT INTO {table} VALUES (?,?,?)", ("invalid", owner, "object.enc"),
                        )
        self.assert_history_unchanged()

    def test_catalog_ownership_survives_a_separate_python_process(self) -> None:
        command = [sys.executable, "-c",
                   "from model import Catalog; from pathlib import Path; import sys; "
                   "c=Catalog(Path(sys.argv[1])); print(c.holdings()[0][1]); "
                   "print(c.work()[0][1]); c.close()", str(self.db_path)]
        result = subprocess.run(command, cwd=HERE, check=True, capture_output=True, text=True)
        self.assertEqual(result.stdout.splitlines(), [self.owner_a, self.owner_a])


if __name__ == "__main__":
    unittest.main()
