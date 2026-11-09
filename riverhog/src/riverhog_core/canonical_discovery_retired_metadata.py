"""Receive original Core child routing under exclusive, irreversible retirement."""

from __future__ import annotations

import os
import re
import sqlite3
import stat
from contextlib import closing

from riverhog_provenance import JournalReplayBinding
from state_schema import read_snapshot

from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_journal_registration import DiscoveryJournalRegistration
from riverhog_core.canonical_discovery_metadata import _FORMAT
from riverhog_core.canonical_discovery_retired_metadata_growth import (
    declare_retired_metadata_growth,
)
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.journal_validation_retirement import RetiringJournalWorkspace
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    _private_directory,
)


class RetiredDiscoveryMetadata:
    """Retain the exact partial handle before opening original retired routing.

    The caller retains this object and its exclusive parent lease through actual
    close receiving. No live worker/admission is reconstructed, and neither an
    empty lookup nor a registered binding supplies physical disposal proof.
    """

    def __init__(
        self, factory: SessionFactory, job: jobs.DiscoveryJob, retirement: RetiringJournalWorkspace
    ) -> None:
        if (
            job.build_id is not None
            or job.state != "retiring"
            or job.storage_state != "confirmed"
            or job.reclamation_phase
            not in {"children", "child_directory", "corpus", "journals_root"}
            or retirement.directory is None
            or retirement.directory.name != job.workspace_id
            or retirement.custody is None
            or retirement.custody.directory != retirement.directory
        ):
            raise StaleArchiveTask("retired Core routing requires its confirmed original custody")
        self.factory, self.job, self.retirement = factory, job, retirement
        self._original = job
        self._custody = retirement.custody
        self._expected = (
            _FORMAT,
            job.workspace_id,
            job.original_build_id,
            job.admission_sha256,
            job.attempt_id,
            job.policy_sha256,
            job.catalog_sha256,
        )
        self._db: sqlite3.Connection | None = None
        self._descriptor: int | None = None
        self._child_root: int | None = None
        self._root_coordinator: int | None = None
        self._initialized = False
        self._closing = False
        self._closed = False

    def _owner_current(self) -> bool:
        if (
            self._closing
            or self._closed
            or self.job != self._original
            or self.retirement.custody is not self._custody
            or self.retirement.directory != self._custody.directory
            or self._custody.directory.name != self.job.workspace_id
            or not self._custody.verify_current()
        ):
            return False
        with read_snapshot(self.factory) as session:
            # This already-retired branch is an exact point read. It cannot
            # detach a live worker or reset a later irreversible phase.
            return jobs.retire_discovery_job(session, self.job) == self._original

    def _file_current(self) -> bool:
        if self._descriptor is None:
            return False
        try:
            held = os.fstat(self._descriptor)
            entry = os.stat("discovery.sqlite3", dir_fd=self._custody._owner, follow_symlinks=False)
        except OSError:
            return False
        return (
            stat.S_ISREG(held.st_mode)
            and stat.S_IMODE(held.st_mode) == 0o600
            and held.st_uid == os.getuid()
            and held.st_nlink == 1
            and (held.st_dev, held.st_ino, held.st_mode)
            == (entry.st_dev, entry.st_ino, entry.st_mode)
        )

    def _header_current(self) -> bool:
        db = self._db
        if db is None:
            return False
        with closing(db.execute("PRAGMA schema_version")) as rows:
            cookie = str(rows.fetchone()[0])
        with closing(
            db.execute(
                "SELECT format,workspace_id,build_id,admission_sha256,attempt_id,"
                "policy_sha256,catalog_sha256,schema_cookie "
                "FROM discovery_workspace WHERE singleton=1"
            )
        ) as rows:
            if rows.fetchone() != (*self._expected, cookie):
                return False
        key = self.job.storage_key
        with closing(
            db.execute(
                "SELECT worker_id FROM discovery_archive_tasks "
                "WHERE build_id=? AND admission_sha256=?",
                (key.build_id, key.admission_sha256),
            )
        ) as rows:
            return bool(rows.fetchone() == (key.worker_id,))

    def verify_current(self) -> bool:
        return (
            self._initialized
            and self._owner_current()
            and self._file_current()
            and self._header_current()
        )

    def _require_current(self) -> sqlite3.Connection:
        if not self.verify_current() or self._db is None:
            raise StaleArchiveTask("retired Core routing lost its exact original custody")
        return self._db

    def open(self) -> None:
        """Open existing confirmed routing; never create, initialize or repair it."""
        if self._initialized:
            self._require_current()
            return
        if self._db is not None or self._descriptor is not None:
            raise LostJournalValidationWorkspace("retired Core opening remains unconfirmed")
        if not self._owner_current():
            raise StaleArchiveTask("retired Core routing has another current disposition")
        self._descriptor = os.open(
            "discovery.sqlite3", os.O_RDWR | os.O_NOFOLLOW, dir_fd=self._custody._owner
        )
        if not self._file_current():
            raise LostJournalValidationWorkspace("retired Core file custody is incompatible")
        path = self._custody.directory / "discovery.sqlite3"
        db = self._db = sqlite3.connect(
            path.as_uri() + "?mode=rw", uri=True, isolation_level=None, check_same_thread=False
        )
        with closing(db.execute("PRAGMA journal_mode")) as rows:
            mode = rows.fetchone()
        with closing(db.execute("PRAGMA auto_vacuum")) as rows:
            vacuum = rows.fetchone()
        if mode != ("delete",) or vacuum != (2,):
            raise LostJournalValidationWorkspace("retired Core storage format changed")
        for statement in (
            "PRAGMA synchronous=FULL",
            "PRAGMA fullfsync=ON",
            "PRAGMA cache_size=-512",
            "PRAGMA temp_store=FILE",
            "PRAGMA foreign_keys=ON",
        ):
            with closing(db.execute(statement)):
                pass
        if not self._owner_current() or not self._file_current() or not self._header_current():
            raise LostJournalValidationWorkspace("retired Core original header is unavailable")
        self._initialized = True

    def child(self) -> DiscoveryJournalRegistration | None:
        """Read one indexed original child; keep its complete Native binding.

        In the durable child-directory phase the catalog retains the selected
        identity even if a prior committed routing deletion lost its reply.
        Absence here never proves namespace completion or permits replacement.
        """
        db = self._require_current()
        key = self.job.storage_key
        query = (
            "SELECT journal_id,worker_id,directory_id,binding_json,confirmed "
            "FROM discovery_journal_children WHERE build_id=? AND admission_sha256=?"
        )
        parameters: tuple[str | None, ...] = (key.build_id, key.admission_sha256)
        if self.job.reclamation_phase == "child_directory":
            query += " AND journal_id=?"
            parameters += (self.job.reclamation_journal_id,)
        else:
            query += " ORDER BY journal_id LIMIT 1"
        with closing(db.execute(query, parameters)) as rows:
            row = rows.fetchone()
        result = None
        if row is not None:
            try:
                binding = JournalReplayBinding.from_json(row[3])
            except (TypeError, RuntimeError) as error:
                raise LostJournalValidationWorkspace("retired child binding is invalid") from error
            if (
                row[1] != key.worker_id
                or row[2] != binding.directory_id
                or re.fullmatch(r"[0-9a-f]{32}", row[2]) is None
                or row[4] not in (0, 1)
                or binding.journal_id != row[0]
                or binding.attempt_id != key.worker_id
                or binding.catalog_sha256 != self.job.catalog_sha256
                or binding.require_profiles
                or (
                    self.job.reclamation_phase == "child_directory"
                    and binding.directory_id != self.job.reclamation_directory_id
                )
            ):
                raise LostJournalValidationWorkspace("retired child has another original binding")
            result = DiscoveryJournalRegistration(binding, bool(row[4]))
        self._require_current()
        return result

    def _receive_child_namespace(self) -> None:
        """Synchronize exact absence without creating or removing any namespace."""
        assert self.job.reclamation_directory_id is not None
        if self._child_root is not None:
            raise LostJournalValidationWorkspace("retired child root closure remains unconfirmed")
        try:
            self._child_root = os.open(
                "journals",
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=self._custody._owner,
            )
        except FileNotFoundError:
            os.fsync(self._custody._owner)
            return
        try:
            identity = _private_directory(self._child_root)
            try:
                os.stat(
                    self.job.reclamation_directory_id,
                    dir_fd=self._child_root,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                pass
            else:
                raise LostJournalValidationWorkspace("selected child namespace still exists")
            os.fsync(self._child_root)
            entry = os.stat("journals", dir_fd=self._custody._owner, follow_symlinks=False)
            if (identity.st_dev, identity.st_ino, identity.st_mode) != (
                entry.st_dev,
                entry.st_ino,
                entry.st_mode,
            ):
                raise LostJournalValidationWorkspace("retired child root was replaced")
        finally:
            try:
                os.close(self._child_root)
            except BaseException:
                self._closing = True
                raise
            self._child_root = None

    def retire_child(self) -> bool:
        """Retire only selected routing after actual namespace absence is received.

        The durable child-directory phase records earlier byte completion and
        keeps the selected identity through lease unlink, rmdir and lost replies.
        This method independently synchronizes exact namespace absence before
        deleting its full original registration. A missing routing row grants no
        namespace proof, and this never advances the separate catalog phase.
        """
        db = self._require_current()
        if self.job.reclamation_phase != "child_directory":
            raise StaleArchiveTask("child routing retirement requires its durable directory phase")
        self._receive_child_namespace()
        with closing(db.execute("BEGIN IMMEDIATE")):
            pass
        try:
            selected = self.child()
            if selected is not None:
                binding = selected.binding
                key = self.job.storage_key
                with closing(
                    db.execute(
                        "DELETE FROM discovery_journal_children WHERE build_id=? "
                        "AND admission_sha256=? AND journal_id=? AND worker_id=? "
                        "AND directory_id=? AND binding_json=? AND confirmed=?",
                        (
                            key.build_id,
                            key.admission_sha256,
                            binding.journal_id,
                            key.worker_id,
                            binding.directory_id,
                            binding.to_json(),
                            int(selected.confirmed),
                        ),
                    )
                ) as rows:
                    if rows.rowcount != 1:
                        raise StaleArchiveTask("selected original child routing changed")
            self._require_current()
            with closing(db.execute("COMMIT")):
                pass
        except BaseException:
            if db.in_transaction:
                try:
                    with closing(db.execute("ROLLBACK")):
                        pass
                except BaseException:
                    self._closing = True
                    raise
            raise
        self._require_current()
        return True

    def _close_namespace_handles(self) -> None:
        """Retain uncertain namespace handles until actual close is received."""
        for name in ("_root_coordinator", "_child_root"):
            descriptor = getattr(self, name)
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except BaseException:
                    self._closing = True
                    raise
                setattr(self, name, None)

    def close(self) -> None:
        """Confirm actual Core closure before the caller ends exclusive custody."""
        self._closing = True
        self._close_namespace_handles()
        if self._db is not None:
            self._db.close()
            self._db = None
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None
        self._closed = True


declare_retired_metadata_growth(RetiredDiscoveryMetadata)
