"""Execute one real publication workload and observe explicit Native cache states."""

from __future__ import annotations

import contextlib
import sqlite3
import sys
import time
import traceback
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from unittest.mock import patch

from riverhog_core.canonical_provenance_archive import (
    _OBJECT_CACHE_BYTES,
    _OBJECT_CACHE_ENTRIES,
    PublishedCanonicalProvenance,
)

from tests.support.qualification.canonical_history_scale import (
    PUBLICATION_POLL_SECONDS,
    PUBLICATION_TIMEOUT_SECONDS,
    publish_shared_history,
)
from tests.support.qualification.publication_evidence import measure_published_history
from tests.support.qualification.publication_geometry import PublicationGeometry, read_workload
from tests.support.runtime_config import write_test_runtime_config
from tests.support.uploaded_archive import UploadedArchiveStore


@dataclass
class _StoredReadCounter:
    original: Callable[[str], bytes]
    calls: int = 0
    octets: int = 0
    paths: set[str] = field(default_factory=set)

    def read(self, path: str) -> bytes:
        raw = self.original(path)
        self.calls += 1
        self.octets += len(raw)
        self.paths.add(path)
        return raw


def run_publication_profile(
    geometry: PublicationGeometry,
    workspace: Path,
    *,
    record_phase: Callable[[dict[str, object]], None],
    finalization_timeout_seconds: int | None = None,
) -> dict[str, object]:
    """Publish once, then verify complete output/history under named reader states.

    The in-process HTTP and in-memory ciphertext store are explicit diagnostic
    conditions. Identity sets and fixture bytes remain in memory; observations
    do not establish production RAM bounds, service throughput or offline recovery.
    Every reported backend read is an actual call to the fixture's stored-byte port.
    The caller owns workspace lifetime and receives the fixture process before
    removing its files, including when a resource close is unconfirmed.
    """
    timeout = (
        PUBLICATION_TIMEOUT_SECONDS
        if finalization_timeout_seconds is None
        else finalization_timeout_seconds
    )
    if type(timeout) is not int or timeout <= 0:
        raise ValueError("publication finalization deadline requires positive seconds")
    directory = workspace
    directory.mkdir(parents=True, exist_ok=True)
    config = write_test_runtime_config(directory)
    with patch.dict("os.environ", {"RIVERHOG_CONFIG": str(config)}):
        record_phase({"phase": "publication", "members": len(geometry.payload_sizes)})
        started = time.perf_counter()
        fixture = publish_shared_history(
            directory / "collection",
            members=len(geometry.payload_sizes),
            geometry=geometry,
            progress=record_phase,
            finalization_timeout_seconds=timeout,
        )
        publication_seconds = time.perf_counter() - started
        try:
            record_phase(
                {
                    "phase": "publication-received",
                    "members": len(fixture.artifact_ids),
                    "publication_seconds": publication_seconds,
                    "archive_root_sha256": fixture.archive_root_sha256,
                }
            )
            archives = fixture.container.provenance._archives
            store = fixture.container.collection_uploads._archive_stores.require("primary").store
            assert isinstance(store, UploadedArchiveStore)
            measurements = []
            for cache_state in (
                "fresh-native-reader",
                "warm-native-reader",
                "evicted-native-reader",
            ):
                if cache_state != "warm-native-reader":
                    archives = PublishedCanonicalProvenance(
                        fixture.container.session_factory,
                        archives._archive_stores,
                        read_order=archives._read_order,
                    )
                    fixture.container.provenance._archives = archives
                record_phase({"phase": "verification", "native_cache_state": cache_state})
                reads = _StoredReadCounter(store.read_stored)
                with patch.object(store, "read_stored", reads.read):
                    observed = measure_published_history(fixture)
                measurements.append(
                    {
                        "native_cache_state": cache_state,
                        "actual_stored_read_calls": reads.calls,
                        "actual_stored_octets": reads.octets,
                        "distinct_stored_objects": len(reads.paths),
                        "actual_stored_octets_per_second": (
                            reads.octets / observed["verification_seconds"]
                        ),
                        "verified_payload_bytes_per_second": (
                            observed["payload_bytes"] / observed["verification_seconds"]
                        ),
                        "verified": observed,
                    }
                )
                record_phase(
                    {
                        "phase": "verification-received",
                        "native_cache_state": cache_state,
                        "verification_seconds": observed["verification_seconds"],
                    }
                )
            workload = asdict(geometry)
            workload["payload_sizes"] = list(geometry.payload_sizes)
            result = {
                "workload": workload,
                "conditions": {
                    "transport": "in-process-TestClient-HTTP",
                    "database": "SQLite",
                    "sqlite_version": sqlite3.sqlite_version,
                    "python_version": ".".join(map(str, sys.version_info[:3])),
                    "ciphertext_store": "in-memory-test-store",
                    "archive_scrypt_work_factor": 1,
                    "producer_cache": False,
                    "append_mode": "one-member-at-a-time",
                    "archive_worker_progress": "bounded-coordinator-polls",
                    "finalization_poll_seconds": PUBLICATION_POLL_SECONDS,
                    "finalization_timeout_seconds": timeout,
                    "native_cache_byte_budget": _OBJECT_CACHE_BYTES,
                    "native_cache_entry_budget": _OBJECT_CACHE_ENTRIES,
                    "cache_scope": "verified Native bytes; OS caches are not cleared",
                    "verification_scope": "full ciphertext checks and Native history per pass",
                    "production_memory_qualification": False,
                    "independent_installed_recovery": False,
                },
                "publication_seconds": publication_seconds,
                "publication_verified_payload_bytes_per_second": (
                    measurements[0]["verified"]["payload_bytes"] / publication_seconds
                ),
                "publication_verified_members_per_second": (
                    measurements[0]["verified"]["members"] / publication_seconds
                ),
                "measurements": measurements,
                "completion_verified": True,
            }
        finally:
            try:
                fixture.api.close()
            finally:
                fixture.container.close()
        record_phase({"phase": "cleanup-received"})
        return result


def main(argv: Sequence[str] | None = None) -> int:
    from riverhog_canonical_json import canonical_json_bytes

    from scripts.validation_identity import source_identity

    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 7:
        return 2
    workload, workspace, completion, progress, run_id, expected_source, timeout = args
    root = Path(__file__).resolve().parents[3]
    source = source_identity(root)
    directory = Path(workspace)
    directory.mkdir(parents=True, exist_ok=True)

    def phase(value: dict[str, object]) -> None:
        record = {
            "format": "riverhog-publication-profile-phase/v1",
            "run_id": run_id,
            **source,
            **value,
        }
        path = Path(progress)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_bytes(canonical_json_bytes(record))
        temporary.replace(path)

    with (directory / "worker.log").open("w") as log:
        with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            try:
                if source != {"source_sha": expected_source, "source_clean": True}:
                    raise ValueError("publication helper source differs")
                observed = run_publication_profile(
                    read_workload(Path(workload)),
                    directory,
                    record_phase=phase,
                    finalization_timeout_seconds=int(timeout) if timeout else None,
                )
                if source_identity(root) != source:
                    raise ValueError("publication helper source changed")
                result = {
                    "format": "riverhog-publication-completion/v1",
                    "run_id": run_id,
                    "source": source,
                    "observed": observed,
                }
                output = Path(completion)
                pending = output.with_name(output.name + ".tmp")
                pending.write_bytes(canonical_json_bytes(result))
                pending.replace(output)
                return 0
            except Exception as error:
                frames = traceback.extract_tb(error.__traceback__)[-6:]
                phase(
                    {
                        "phase": "failed",
                        "error_type": type(error).__name__,
                        "source_frames": [
                            {
                                "file": Path(frame.filename).name,
                                "line": frame.lineno,
                                "function": frame.name,
                            }
                            for frame in frames
                        ],
                    }
                )
                return 1


if __name__ == "__main__":
    raise SystemExit(main())
