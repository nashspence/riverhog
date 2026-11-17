"""Actual-owner #999 experiment; no production implementation or closure proof.

Only the copy progress method changes. Both arms run the real copy service,
catalog and original encrypted archive fixture. Provider bodies are controlled
in-memory fixtures. SQLAlchemy commits are counted, not physical fsync calls.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from riverhog_core.catalog_models import (
    ArchiveCopyObjectUploadRecord,
    CollectionArchiveCopyRecord,
)
from riverhog_core.services.archive_copy_jobs import SqlAlchemyArchiveCopyJobService
from riverhog_protocol.errors import Conflict
from sqlalchemy import event
from state_schema import read_snapshot

from tests.unit.archive_object_fixtures import COLLECTION_ID, FixtureArchive, MemoryArchiveStore
from tests.unit.test_archive_copy_jobs import INITIATOR, _multiple_archive_parts, _service

SOURCE = Path(__file__).resolve().parents[2]
SOURCE_COMMIT = "c28eceaa2dfec1b3712135469ba12230e74f5ffa"
Mode = Literal["current", "volatile"]


class SimulatedInterruption(BaseException):
    """Raised after a selected boundary; real worker futures still join."""


@dataclass
class SqlCounts:
    statements: int = 0
    mutations: int = 0
    progress_updates: int = 0
    commits: int = 0
    rollbacks: int = 0


class SqlObservation:
    def __init__(self, service: SqlAlchemyArchiveCopyJobService) -> None:
        self.engine = service._session_factory.kw["bind"]
        self.counts = SqlCounts()
        self.listeners = (
            ("before_cursor_execute", self.statement),
            ("commit", self.commit),
            ("rollback", self.rollback),
        )

    def __enter__(self) -> SqlObservation:
        for name, callback in self.listeners:
            event.listen(self.engine, name, callback)
        return self

    def __exit__(self, *_: object) -> None:
        for name, callback in self.listeners:
            event.remove(self.engine, name, callback)

    def statement(self, _conn: object, _cursor: object, statement: str, *_: object) -> None:
        sql = statement.lstrip().upper()
        self.counts.statements += 1
        self.counts.mutations += sql.startswith(("INSERT ", "UPDATE ", "DELETE "))
        self.counts.progress_updates += sql.startswith(
            "UPDATE ARCHIVE_COPY_OBJECT_UPLOADS SET UPLOADED_BYTES="
        )

    def commit(self, _: object) -> None:
        self.counts.commits += 1

    def rollback(self, _: object) -> None:
        self.counts.rollbacks += 1


class ObservedCopyService(SqlAlchemyArchiveCopyJobService):
    """One active object's advisory slot; unchanged original receipt ownership.

    This deliberately does not implement a public overlay, new policy budget,
    authorization fencing or provider invocation persistence. The read-only
    point lookup retains the original missing-upload rejection.
    """

    def __init__(self, original: SqlAlchemyArchiveCopyJobService, mode: Mode) -> None:
        super().__init__(
            original._config,
            original._archive_stores,
            retrieval_cache=original._retrieval_cache,
            session_factory=original._session_factory,
            throughput_tuning=original._throughput,
            transfer_resources=original._resources,
        )
        self.mode = mode
        self.progress_calls = 0
        self.progress_elapsed_ns = 0
        self.latest: tuple[tuple[int, str, str], tuple[int, int, int]] | None = None
        self.interrupt_after_progress = False
        self.interrupt_before_object_receipt = False

    def _record_copy_progress(
        self,
        *,
        collection_id: int,
        destination_store: str,
        object_id: str,
        uploaded_bytes: int,
        uploaded_segments: int,
        total_segments: int,
    ) -> None:
        started = time.monotonic_ns()
        key = (collection_id, destination_store, object_id)
        try:
            if self.mode == "current":
                super()._record_copy_progress(
                    collection_id=collection_id,
                    destination_store=destination_store,
                    object_id=object_id,
                    uploaded_bytes=uploaded_bytes,
                    uploaded_segments=uploaded_segments,
                    total_segments=total_segments,
                )
            else:
                with read_snapshot(self._session_factory) as session:
                    if session.get(ArchiveCopyObjectUploadRecord, key) is None:
                        raise Conflict("archive copy upload checkpoint disappeared")
                self.latest = (key, (uploaded_bytes, uploaded_segments, total_segments))
            self.progress_calls += 1
        finally:
            self.progress_elapsed_ns += time.monotonic_ns() - started
        if self.interrupt_after_progress:
            self.interrupt_after_progress = False
            raise SimulatedInterruption("after progress, before object completion")

    def _record_copied_object(self, **kwargs: Any) -> None:
        if self.interrupt_before_object_receipt:
            self.interrupt_before_object_receipt = False
            raise SimulatedInterruption("sealed provider object, before catalog receipt")
        super()._record_copied_object(**kwargs)
        self.latest = None


class ObservedDestination(MemoryArchiveStore):
    def __init__(self) -> None:
        super().__init__(new_archive_prefix="archives/b2/new-copy")
        self.segment_calls = 0

    def write_segment(self, **kwargs: Any) -> Any:
        self.segment_calls += 1
        return super().write_segment(**kwargs)


def fixture_archive(parts: int = 16) -> FixtureArchive:
    return _multiple_archive_parts(
        {"d" * 64: bytes(range(251)) * 256, "e" * 64: b"exact original archive bytes\n"},
        parts=parts,
    )


def copy_case(
    path: Path,
    archive: FixtureArchive,
    mode: Mode,
    *,
    destination: MemoryArchiveStore | None = None,
) -> tuple[ObservedCopyService, MemoryArchiveStore, MemoryArchiveStore]:
    path.mkdir(parents=True, exist_ok=True)
    _config, _archive, source, target, original = _service(
        path / "catalog.sqlite3",
        archive=archive,
        destination=destination or ObservedDestination(),
    )
    service = ObservedCopyService(original, mode)
    service.create_or_resume(
        COLLECTION_ID,
        source_store="deep",
        destination_store="b2",
        initiator=INITIATOR,
    )
    return service, source, target


def exact_evidence(service: ObservedCopyService, destination: MemoryArchiveStore) -> dict[str, Any]:
    with read_snapshot(service._session_factory) as session:
        copy = session.get(CollectionArchiveCopyRecord, (COLLECTION_ID, "b2"))
        assert copy is not None
        rows = [
            {
                name: getattr(row, name)
                for name in (
                    "object_id",
                    "kind",
                    "object_path",
                    "revision",
                    "stored_bytes",
                    "plaintext_bytes",
                    "sha256",
                    "stored_sha256",
                    "archive_parts_json",
                    "age_state_json",
                    "plan_sha256",
                    "index_sha256",
                )
            }
            for row in copy.objects
        ]
        placements = [
            {
                name: getattr(placement, name)
                for name in (
                    "artifact_id",
                    "sequence",
                    "object_id",
                    "artifact_offset",
                    "object_offset",
                    "bytes",
                    "member",
                )
            }
            for row in copy.objects
            for placement in row.placements
        ]
        state = copy.state
    return {
        "state": state,
        "objects": rows,
        "placements": placements,
        "stored_bytes_sha256": {
            path: hashlib.sha256(value).hexdigest()
            for path, value in sorted(destination.objects.items())
        },
        "metadata": destination.object_metadata,
        "content_types": destination.object_content_types,
    }


def run_pair(path: Path, *, parts: int) -> dict[str, Any]:
    archive = fixture_archive(parts)
    outputs: dict[str, Any] = {}
    evidence: dict[str, Any] = {}
    for mode in ("current", "volatile"):
        service, source, destination = copy_case(path / mode, archive, mode)
        with SqlObservation(service) as sql:
            started = time.monotonic_ns()
            processed = service.process_due(limit=1)
            elapsed_ns = time.monotonic_ns() - started
        assert processed == 1
        assert service.get(COLLECTION_ID, destination_store="b2")["state"] == "completed"
        assert destination.objects == {
            f"archives/b2/new-copy/{relative}": content
            for relative, content in archive.stored_objects.items()
        }
        evidence[mode] = exact_evidence(service, destination)
        outputs[mode] = {
            "elapsed_ns": elapsed_ns,
            "sql": asdict(sql.counts),
            "progress_calls": service.progress_calls,
            "progress_elapsed_ns": service.progress_elapsed_ns,
            "source_object_reads": len(source.read),
            "segment_calls": destination.segment_calls,
            "provider_sessions_created": destination._next_write - 1,
        }
    assert evidence["current"] == evidence["volatile"]
    return {
        "archive_parts": parts,
        "archive_objects": len(archive.stored_objects),
        "stored_bytes": sum(map(len, archive.stored_objects.values())),
        "exact_evidence_equal": True,
        "evidence_sha256": hashlib.sha256(
            json.dumps(evidence["current"], sort_keys=True).encode()
        ).hexdigest(),
        "arms": outputs,
    }


def s3_scan_fixture(*, parts: int = 2050) -> tuple[Any, Any, Any]:
    """Real S3 traversal against metadata-only, paginated provider fixture."""
    from a_riverhog_s3_store_lib import S3StorageAdapter
    from riverhog_storage_adapter_protocol import WriteSession

    path = SOURCE / (
        "some-implementations/riverhog/storage/s3-support/tests/test_s3_storage_adapter.py"
    )
    name = "reference_999_original_s3_fixture"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    original = importlib.util.module_from_spec(spec)
    sys.modules[name] = original
    spec.loader.exec_module(original)

    class PagedProvider(original._FakeS3Client):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0
            self.rows = 0
            self.changed = False

        def list_parts(self, **request: Any) -> dict[str, Any]:
            self.calls += 1
            after = int(request.get("PartNumberMarker", 0))
            stop = min(after + 1000, parts)
            values = [
                {
                    "PartNumber": number,
                    "ETag": f'"part-{number}-{self.changed and number == 1}"',
                    "Size": 5 * 1024 * 1024,
                }
                for number in range(after + 1, stop + 1)
            ]
            self.rows += len(values)
            return {
                "Parts": values,
                "IsTruncated": stop < parts,
                "NextPartNumberMarker": stop,
            }

    client = PagedProvider()
    adapter = S3StorageAdapter(client, original._config())
    session = WriteSession(
        object_path="objects/fixture.age",
        write_token="original-fixture-upload",
        expected_bytes=parts * 5 * 1024 * 1024,
    )
    return adapter, client, session


def measure_s3_scan() -> dict[str, int]:
    from riverhog_storage_adapter_protocol import WriteSegmentListRequest

    adapter, provider, session = s3_scan_fixture()
    after = 0
    token = None
    pages = 0
    started = time.monotonic_ns()
    while True:
        page = adapter.list_segments(
            WriteSegmentListRequest(
                session=session,
                after_number=after,
                traversal_token=token,
                maximum_items=128,
            )
        )
        pages += 1
        if page.next_after_number is None:
            assert page.completion is not None
            break
        after, token = page.next_after_number, page.traversal_token
    return {
        "parts": 2050,
        "public_page_maximum": 128,
        "public_pages": pages,
        "provider_list_parts_calls": provider.calls,
        "provider_metadata_rows": provider.rows,
        "elapsed_ns": time.monotonic_ns() - started,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.workspace.mkdir(parents=True, exist_ok=False)
    report = {
        "format": "riverhog-999-adapter-experiment/v1",
        "source_commit": SOURCE_COMMIT,
        "scope": "real Core owners; controlled provider fixtures; no live provider or fsync trace",
        "timing_scope": "copy process_due only; excludes fixture construction and evidence queries",
        "pairs": [run_pair(args.workspace / str(parts), parts=parts) for parts in (16, 64)],
        "s3_current_traversal": measure_s3_scan(),
        "experiment_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
