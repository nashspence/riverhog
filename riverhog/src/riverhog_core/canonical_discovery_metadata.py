"""Owned Core metadata beneath the current discovery worker's kernel custody."""

from __future__ import annotations

import os
import sqlite3
import stat
from collections.abc import Iterator
from contextlib import closing, contextmanager

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import (
    ArchiveDiscoveryAdmission,
    ArchiveTaskKey,
    StaleArchiveTask,
    begin_archive_metadata,
    initialize_archive_work,
)
from riverhog_core.canonical_discovery_corpus_replay import initialize_corpus_replay
from riverhog_core.canonical_discovery_endpoint_index import initialize_endpoint_index
from riverhog_core.canonical_discovery_journal_schema import (
    initialize_archive_framing,
    initialize_journal_epochs,
    initialize_journal_preparation,
    initialize_journal_rank,
    initialize_journal_registration,
    initialize_journal_topology,
    initialize_journal_union,
)
from riverhog_core.canonical_discovery_primary_work import initialize_primary_work
from riverhog_core.canonical_discovery_selection import initialize_corpus_selection
from riverhog_core.canonical_discovery_selector_work import initialize_selector_work
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    ValidationWorkspaceCustody,
)

_FORMAT = "riverhog-discovery-metadata/v15"


class DiscoveryMetadata:
    """Keep one worker's Core handle until its actual close is confirmed.

    The caller retains this object before opening storage and keeps the original
    lease until close returns. An uncertain close blocks work and remains
    retryable; it never releases the caller's kernel custody.
    """

    def __init__(
        self,
        factory: SessionFactory,
        job: jobs.DiscoveryJob,
        admission: ArchiveDiscoveryAdmission,
        custody: ValidationWorkspaceCustody,
    ) -> None:
        if (
            job.worker_epoch < 1
            or job.workspace_id != custody.directory.name
            or job.admission_sha256 != admission.identity
            or job.archive_generation != admission.root.archive_generation
            or job.archive_root_sha256 != admission.archive_sha256
            or job.provenance_identity != admission.provenance_sha256
            or job.artifact_set_identity != admission.root.artifact_set_sha256
            or job.journal_count != str(admission.root.journal_count)
        ):
            raise StaleArchiveTask("Core metadata has another claimed archive attempt")
        self.factory, self.job, self.admission, self.custody = factory, job, admission, custody
        self._expected = self._binding()
        self._claimed = self._claimed_binding()
        self._root = admission.root
        self._archive_binding = (
            admission.identity,
            admission.archive_sha256,
            admission.provenance_sha256,
            admission.binding_json,
            admission.root_json,
        )
        self._db: sqlite3.Connection | None = None
        self._descriptor: int | None = None
        self._initialized = False
        self._closing = False
        self._closed = False

    @property
    def key(self) -> ArchiveTaskKey:
        return self.job.key

    def _binding(self) -> tuple[str, ...]:
        return (
            _FORMAT,
            self.job.workspace_id,
            self.job.build_id or "",
            self.job.admission_sha256,
            self.job.attempt_id,
            self.job.policy_sha256,
            self.job.catalog_sha256,
        )

    def _claimed_binding(self) -> tuple[str | int, ...]:
        return (
            self.job.archive_generation,
            self.job.archive_root_sha256,
            self.job.provenance_identity,
            self.job.artifact_set_identity,
            self.job.journal_count,
            self.job.build_epoch,
            self.job.worker_epoch,
        )

    def _owner_current(self) -> bool:
        return (
            not self._closing
            and not self._closed
            and self._binding() == self._expected
            and self._claimed_binding() == self._claimed
            and self.admission.root is self._root
            and (
                self.admission.identity,
                self.admission.archive_sha256,
                self.admission.provenance_sha256,
                self.admission.binding_json,
                self.admission.root_json,
            )
            == self._archive_binding
            and self.custody.verify_current()
            and jobs.discovery_worker_current(self.factory, self.job)
        )

    def _file_current(self) -> bool:
        if self._descriptor is None:
            return False
        try:
            held = os.fstat(self._descriptor)
            entry = os.stat(self.custody.directory / "discovery.sqlite3", follow_symlinks=False)
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
        with closing(
            db.execute(
                "SELECT binding_json=? AND root_json=? AND worker_id=? "
                "FROM discovery_archive_tasks WHERE build_id=? AND admission_sha256=?",
                (
                    self.admission.binding_json,
                    self.admission.root_json,
                    self.key.worker_id,
                    self.key.build_id,
                    self.key.admission_sha256,
                ),
            )
        ) as rows:
            return bool(rows.fetchone() == (1,))

    def verify_current(self) -> bool:
        return (
            self._initialized
            and self._owner_current()
            and self._file_current()
            and self._header_current()
        )

    def _require_current(self) -> sqlite3.Connection:
        if not self.verify_current() or self._db is None:
            raise StaleArchiveTask("Core metadata no longer has current owned custody")
        return self._db

    def open(self) -> None:
        """Open only the selected private file; initialize only unconfirmed storage."""
        if self._initialized:
            self._require_current()
            return
        if self._db is not None or self._descriptor is not None:
            raise LostJournalValidationWorkspace("Core metadata opening has an unconfirmed outcome")
        if not self._owner_current():
            raise StaleArchiveTask("Core metadata worker is no longer current")
        current = jobs.discovery_worker_snapshot(self.factory, self.job)
        if current is None:
            raise StaleArchiveTask("Core metadata has no current worker disposition")
        self.job = current
        path = self.custody.directory / "discovery.sqlite3"
        created = False
        if self.job.state == "preparing":
            try:
                self._descriptor = os.open(
                    path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
                )
            except FileExistsError:
                self._descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
            else:
                created = True
        else:
            self._descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
        if not self._file_current():
            raise LostJournalValidationWorkspace("Core metadata file custody is incompatible")
        db = self._db = sqlite3.connect(
            path.as_uri() + "?mode=rw", uri=True, isolation_level=None, check_same_thread=False
        )
        if created:
            with closing(db.execute("PRAGMA auto_vacuum=INCREMENTAL")):
                pass
            with closing(db.execute("PRAGMA journal_mode=DELETE")) as rows:
                if rows.fetchone() != ("delete",):
                    raise LostJournalValidationWorkspace("Core metadata journal mode unavailable")
        else:
            with closing(db.execute("PRAGMA journal_mode")) as rows:
                mode = rows.fetchone()
            with closing(db.execute("PRAGMA auto_vacuum")) as rows:
                vacuum = rows.fetchone()
            if mode != ("delete",) or vacuum != (2,):
                raise LostJournalValidationWorkspace("Core metadata storage format changed")
        for statement in (
            "PRAGMA synchronous=FULL",
            "PRAGMA fullfsync=ON",
            "PRAGMA cache_size=-512",
            "PRAGMA temp_store=FILE",
            "PRAGMA foreign_keys=ON",
        ):
            with closing(db.execute(statement)):
                pass
        if created:
            initialize_archive_work(db)
            initialize_archive_framing(db)
            initialize_endpoint_index(db)
            initialize_selector_work(db)
            initialize_corpus_selection(db)
            initialize_primary_work(db)
            initialize_corpus_replay(db)
            initialize_journal_registration(db)
            initialize_journal_epochs(db)
            initialize_journal_topology(db)
            initialize_journal_union(db)
            initialize_journal_rank(db)
            initialize_journal_preparation(db)
            with closing(
                db.execute(
                    "CREATE TABLE discovery_workspace("
                    "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                    "format TEXT NOT NULL,workspace_id TEXT NOT NULL,build_id TEXT NOT NULL,"
                    "admission_sha256 TEXT NOT NULL,attempt_id TEXT NOT NULL,"
                    "policy_sha256 TEXT NOT NULL,catalog_sha256 TEXT NOT NULL,"
                    "schema_cookie TEXT NOT NULL)"
                )
            ):
                pass
            with closing(db.execute("BEGIN IMMEDIATE")):
                pass
            try:
                begin_archive_metadata(db, self.key, self.admission)
                with closing(db.execute("PRAGMA schema_version")) as rows:
                    cookie = str(rows.fetchone()[0])
                with closing(
                    db.execute(
                        "INSERT INTO discovery_workspace VALUES(1,?,?,?,?,?,?,?,?)",
                        (*self._expected, cookie),
                    )
                ):
                    pass
                if not self._owner_current() or not self._file_current():
                    raise StaleArchiveTask("Core metadata custody changed during initialization")
                db.commit()
            except BaseException:
                db.rollback()
                raise
            assert self._descriptor is not None
            os.fsync(self._descriptor)
            directory = os.open(
                self.custody.directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        if not self._owner_current() or not self._file_current() or not self._header_current():
            raise LostJournalValidationWorkspace(
                "Core metadata initialization is missing or changed"
            )
        confirmed = jobs.confirm_discovery_storage(self.factory, self.job)
        if confirmed is None:
            raise StaleArchiveTask("Core metadata storage confirmation lost its current worker")
        self.job = confirmed
        self._initialized = True
        self._require_current()

    @contextmanager
    def read(self) -> Iterator[tuple[sqlite3.Connection, ArchiveTaskKey]]:
        db = self._require_current()
        yield db, self.key
        self._require_current()

    @contextmanager
    def transaction(self) -> Iterator[tuple[sqlite3.Connection, ArchiveTaskKey]]:
        db = self._require_current()
        with closing(db.execute("BEGIN IMMEDIATE")):
            pass
        try:
            yield db, self.key
            self._require_current()
            db.commit()
        except BaseException:
            db.rollback()
            raise

    def close(self) -> None:
        """Confirm Core closure before the caller releases the selected kernel lease."""
        self._closing = True
        if self._db is not None:
            self._db.close()
            self._db = None
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None
        self._closed = True


def _metadata_declaration(operation: str) -> Declaration:
    opening = operation == "open"
    closing_storage = operation == "close"
    fences = 0 if closing_storage else 4 if opening else 1 if operation == "verify_current" else 2

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def octets(value: int) -> Expression:
        return Expression("constant", value=value, unit=OCTETS)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def width(value: int) -> Expression:
        return scale(value, Expression("dimension", name="control_octets"))

    def logarithm(name: str) -> Expression:
        return Expression(
            "log2_ceil",
            terms=(Expression("sum", terms=(Expression("dimension", name=name), count(2))),),
        )

    catalog_calls = fences + (4 if opening else 0)
    core_statements = 192 if opening else 0 if closing_storage else fences * 3 + 2
    core_rows = 16 if opening else 0 if closing_storage else fences * 3
    limits = {
        "core_sql_statements": core_statements,
        "core_returned_rows": core_rows,
        "core_changed_rows": 3 if opening else 0,
        "core_opened_connections": int(opening),
        "core_closed_connections": int(closing_storage),
        "catalog_sql_statements": catalog_calls,
        "catalog_driver_statements": catalog_calls * 33,
        "catalog_returned_rows": catalog_calls * 16,
        "catalog_changed_rows": int(opening),
        "uuid_values": int(opening),
        "open_calls": 2 if opening else 0,
        "close_calls": int(opening or closing_storage),
        "stat_calls": fences * 8,
        "uid_calls": fences * 4,
        "fsync_calls": 2 if opening else 0,
        "directory_enumerations": 0,
    }
    return Declaration(
        id="riverhog-server.discovery-metadata-" + operation + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_metadata.DiscoveryMetadata." + operation,),
        scope=(
            "owned Core metadata opening, exact currentness, context fencing or confirmed closure"
        ),
        starts_at=(
            "receive the current claimed job, authenticated archive and actual held lease custody"
        ),
        ends_at="return or reject the owned operation without releasing the caller's kernel lease",
        dimensions=(
            Dimension("retained_core_rows", COUNT, "Independent retained Core work rows."),
            Dimension("retained_catalog_rows", COUNT, "Independent original catalog and job rows."),
            Dimension(
                "control_octets",
                OCTETS,
                "Complete job, archive header and incoming control widths.",
            ),
            Dimension("path_octets", OCTETS, "Complete native encoded selected file path width."),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper",
                    count(value),
                    "Fixed current schema, point fences, connection primitives and exact custody.",
                )
                for metric, value in limits.items()
            ),
            Bound(
                "core_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(65536 if opening else fences * 4096),
                        Expression(
                            "product",
                            terms=(count(512 * max(fences, 1)), logarithm("retained_core_rows")),
                        ),
                    ),
                ),
                "Fixed schema creation or indexed singleton/archive-parent "
                "lookups; no row traversal.",
            ),
            Bound(
                "catalog_vm_instructions",
                COUNT,
                "upper",
                scale(
                    catalog_calls,
                    Expression(
                        "sum",
                        terms=(
                            count(32768),
                            Expression(
                                "product", terms=(count(2048), logarithm("retained_catalog_rows"))
                            ),
                        ),
                    ),
                ),
                "Actual point catalog fences and optional confirmed storage transition.",
            ),
            Bound(
                "core_returned_octets",
                OCTETS,
                "upper",
                width(core_rows),
                "Complete original header and backend control results.",
            ),
            Bound(
                "core_parameter_octets",
                OCTETS,
                "upper",
                width(32 if opening else fences * 4),
                "Original root preimages and exact keys are bound a fixed number of times.",
            ),
            Bound(
                "catalog_returned_octets",
                OCTETS,
                "upper",
                width(catalog_calls * 16),
                "Full catalog rows, not abbreviated labels.",
            ),
            Bound(
                "catalog_parameter_octets",
                OCTETS,
                "upper",
                width(catalog_calls * 68),
                "Complete job and exact original build/root predicates.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                octets(256 if opening else 0),
                "Only fixed initialization domain bytes and one catalog version UUID are hashed.",
            ),
            Bound(
                "encoded_octets",
                OCTETS,
                "upper",
                width(1) if opening else octets(0),
                "Serialize the initial archive hash continuation once; its complete width "
                "is included in the original control metadata.",
            ),
            Bound(
                "encoded_path_octets",
                OCTETS,
                "upper",
                scale(
                    fences * 5 + (6 if opening else 0), Expression("dimension", name="path_octets")
                ),
                "Exact selected root, directory, lease and Core file paths only.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                octets(0),
                "No archived journal/member payload is replayed by this storage boundary.",
            ),
        ),
        conditions=(
            "The caller serializes opening, contexts and closure for this owned attempt.",
            "The caller retains the original exclusive kernel lease and this "
            "object before opening.",
            "Archive admission authentication and constructed catalog identity "
            "precede this boundary.",
            "The maintained SQLite schema has its declared indexes and no additional triggers.",
            "Read/transaction context body work retains its own declarations and "
            "composed observations.",
            "Logical SQLite instructions exclude physical I/O, parser internals "
            "and B-tree comparisons.",
        ),
        invariants=(
            "Only preparing storage may be created; accepted missing or "
            "incompatible files are not repaired.",
            "Current catalog worker, held descriptors, original header and exact "
            "file identity fence work.",
            "A stale final fence rolls back the context body rather than "
            "publishing partial progress.",
            "An uncertain close blocks new work and preserves the caller's kernel"
            " custody for retry.",
        ),
        limitations=(
            "Physical SQLite/filesystem I/O, native parser internals and B-tree "
            "comparisons are excluded.",
            "Caller context bodies retain separately measured archive/Native operation costs.",
        ),
    )


for _operation in ("open", "verify_current", "read", "transaction", "close"):
    declare_growth(_metadata_declaration(_operation))(getattr(DiscoveryMetadata, _operation))
