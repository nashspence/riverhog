"""Reference-only experiment over the real operation-local provenance index.

This is not a retained Native generation, index rebuild executor or production
patch. The existing reader still verifies the complete archive sequence before
calling seal(). The only proposed change keeps disposable rows in the original
connection's private transaction until that original connection is received.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import patch

from riverhog_archive_contracts import ProvenanceRootDocument
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import provenance_archive_read as archive_read
from riverhog_core.canonical_discovery_rebuild import _ArchiveCorpus
from riverhog_core.provenance_catalog import admission_provenance_catalog
from riverhog_core.provenance_metadata_index import ProvenanceMetadataIndex
from riverhog_core.scratch_workspace import capacity_errors
from riverhog_provenance import JournalSummary, validate_journal_chunks


class PrivateReadIndex(ProvenanceMetadataIndex):
    """Same disposable owner; no commit or resumable state is selected."""

    def seal(self) -> None:
        self.require_open()
        with capacity_errors():
            self._run("PRAGMA query_only = ON")
            self.require_open()
            self._ready = True


class ReceivingArchiveCorpus(_ArchiveCorpus):
    """Two usable summaries plus at most one unresolved eviction; no replacement."""

    closing = False

    def __getitem__(self, journal_id: str) -> JournalSummary:
        if self.closing:
            raise RuntimeError("corpus receiving is incomplete")
        if journal_id not in self.cache:
            self.cache[journal_id] = validate_journal_chunks(
                self.reader.iter_journal_range(journal_id),
                catalog=admission_provenance_catalog(),
                require_profiles=False,
            )
        self.cache.move_to_end(journal_id)
        if len(self.cache) > 2:
            key, original = next(iter(self.cache.items()))
            try:
                original.close()
                if not original.storage_received:
                    raise RuntimeError("original summary storage remains unreceived")
            except BaseException:
                self.closing = True
                raise
            del self.cache[key]
        return self.cache[journal_id]

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        self.closing = True
        errors = []
        for key, original in tuple(self.cache.items()):
            try:
                original.close()
                if not original.storage_received:
                    raise RuntimeError("original summary storage remains unreceived")
                del self.cache[key]
            except BaseException as error:
                errors.append(error)
        if errors:
            raise errors[0]


@dataclass
class Measurements:
    explicit_commit_requests: int = 0
    traced_commit_statements: int = 0
    inserted_descriptors: int = 0
    original_close_attempts: int = 0
    original_close_received: bool = False
    sql_statements: Counter[str] = field(default_factory=Counter)
    archive_reads: Counter[str] = field(default_factory=Counter)
    scratch_paths: list[str] = field(default_factory=list)

    def trace(self, statement: str) -> None:
        operation = statement.strip().split()[0].upper()
        self.sql_statements[operation] += 1
        self.traced_commit_statements += operation == "COMMIT"
        self.inserted_descriptors += statement.startswith("INSERT INTO volumes ")


class ObservedConnection(sqlite3.Connection):
    observations: Measurements
    fail_close: str | None = None

    def commit(self) -> None:
        self.observations.explicit_commit_requests += 1
        super().commit()

    def close(self) -> None:
        self.observations.original_close_attempts += 1
        failure, self.fail_close = self.fail_close, None
        if failure == "before":
            raise OSError("original close unconfirmed")
        super().close()
        if failure == "after":
            raise OSError("original close reply lost")
        self.observations.original_close_received = True


def instrumented_reader(
    objects: dict[str, bytes], *, fail_close: str | None = None
) -> tuple[archive_read.CanonicalProvenanceArchiveReader, Measurements]:
    """The fixture supplies the authenticated-plaintext ObjectReader boundary."""
    observations = Measurements()
    root = ProvenanceRootDocument.from_json_bytes(objects["provenance/root.json.age"])

    def read_object(path: str):
        observations.archive_reads[path] += 1
        yield objects[path]

    def connect(path: str | Path, **kwargs: Any) -> sqlite3.Connection:
        connection = sqlite3.connect(path, **kwargs, factory=ObservedConnection)
        connection.observations = observations
        connection.fail_close = fail_close
        connection.set_trace_callback(observations.trace)
        observations.scratch_paths.append(str(path))
        return connection

    return (
        archive_read.CanonicalProvenanceArchiveReader(
            read_object,
            expected_root_sha256=root.identity,
            archive_generation=root.archive_generation,
            artifact_set_sha256=root.artifact_set_sha256,
            metadata_connection_factory=connect,
        ),
        observations,
    )


def observe(
    objects: dict[str, bytes], journal_id: str, *, private: bool, reads: int = 3
) -> dict[str, object]:
    """Run real complete preparation and repeated original journal/binding reads."""
    reader, observations = instrumented_reader(objects)
    index_type = PrivateReadIndex if private else ProvenanceMetadataIndex
    identities = []
    with patch.object(archive_read, "ProvenanceMetadataIndex", index_type), reader.prepared():
        owner = reader.retained_metadata_storage
        assert owner is not None
        rows_ready_before_reads = owner._ready
        transaction_open_while_reading = owner._db.in_transaction
        query_only = owner._db.execute("PRAGMA query_only").fetchone() == (1,)
        metadata_reads_after_prepare = sum(
            count for path, count in observations.archive_reads.items() if "/metadata/" in path
        )
        for _ in range(reads):
            headers = tuple(reader.iter_journal_headers())
            journal = b"".join(reader.iter_journal_range(journal_id))
            bindings = tuple(reader.iter_bindings())
            identities.append(
                {
                    "headers": headers,
                    "journal_bytes": len(journal),
                    "journal_sha256": hashlib.sha256(journal).hexdigest(),
                    "bindings_sha256": hashlib.sha256(
                        canonical_json_bytes(list(bindings))
                    ).hexdigest(),
                }
            )
        metadata_reads_after_use = sum(
            count for path, count in observations.archive_reads.items() if "/metadata/" in path
        )
    assert reader.retained_metadata_storage is None
    measurements = asdict(observations)
    measurements["sql_statements"] = dict(observations.sql_statements)
    measurements["archive_reads"] = dict(observations.archive_reads)
    return {
        "mode": "proposed-private" if private else "unchanged",
        "measurements": measurements,
        "rows_ready_before_reads": rows_ready_before_reads,
        "query_only": query_only,
        "transaction_open_while_reading": transaction_open_while_reading,
        "metadata_reads_after_prepare": metadata_reads_after_prepare,
        "metadata_reads_after_use": metadata_reads_after_use,
        "results": identities,
        "scratch_removed": all(
            not Path(path).parent.exists() for path in observations.scratch_paths
        ),
        "scope": (
            "Real Core reader/index/SQLite on fixture authenticated plaintext objects; "
            "no retained Native generation, archive provider, fsync count, power-loss, "
            "PostgreSQL or installed recovery claim."
        ),
    }
