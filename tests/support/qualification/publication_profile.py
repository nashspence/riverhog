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
from uuid import uuid4

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


@dataclass
class _PublicationTimeline:
    fixture_started_ns: int
    clock_origin_id: str = field(default_factory=lambda: str(uuid4()))
    admission_ns: int | None = None
    result_ns: int | None = None
    receiving_ns: int | None = None
    archive_root_sha256: str | None = None

    def observe(self, event: dict[str, object]) -> None:
        phase = event.get("phase")
        if phase not in {
            "publication-admitted",
            "publication-result-received",
            "publication-resources-received",
        }:
            return
        now = time.perf_counter_ns()
        if now < self.fixture_started_ns:
            raise ValueError("publication clock moved before fixture admission")
        if phase == "publication-admitted":
            if self.admission_ns is not None:
                raise ValueError("publication admission was observed twice")
            self.admission_ns = now
            return
        if self.admission_ns is None or now < self.admission_ns:
            raise ValueError("publication milestone has no original admission clock")
        root = event.get("archive_root_sha256")
        if (
            not isinstance(root, str)
            or len(root) != 64
            or any(c not in "0123456789abcdef" for c in root)
        ):
            raise ValueError("publication milestone has no exact result identity")
        if phase == "publication-result-received":
            if self.result_ns is not None or self.receiving_ns is not None:
                raise ValueError("publication result was observed twice or after receiving")
            self.result_ns, self.archive_root_sha256 = now, root
            return
        if self.result_ns is None or now < self.result_ns or root != self.archive_root_sha256:
            raise ValueError("publication receiving differs from its original result")
        if self.receiving_ns is not None:
            raise ValueError("publication receiving was observed twice")
        self.receiving_ns = now

    def verified(self, archive_root_sha256: str) -> dict[str, object]:
        if (
            self.admission_ns is None
            or self.result_ns is None
            or self.receiving_ns is None
            or self.archive_root_sha256 != archive_root_sha256
        ):
            raise ValueError("publication workflow lacks its original result or receiving")
        clock = time.get_clock_info("perf_counter")
        return {
            "clock": {
                "name": "perf_counter_ns",
                "implementation": clock.implementation,
                "resolution_seconds": clock.resolution,
                "monotonic": clock.monotonic,
                "origin_id": self.clock_origin_id,
                "origin_ns": self.admission_ns,
            },
            "archive_root_sha256": archive_root_sha256,
            "fixture_preparation_ns": self.admission_ns - self.fixture_started_ns,
            "result_elapsed_ns": self.result_ns - self.admission_ns,
            "receiving_elapsed_ns": self.receiving_ns - self.admission_ns,
            "post_result_tail_ns": self.receiving_ns - self.result_ns,
            "result_boundary": "verified-original-producer-finish-receipt",
            "receiving_boundary": "producer-primary-and-core-publication-owners-closed",
            "receiving_scope": "isolated publication owners; excludes fixture catalog and readers",
        }


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
        timeline = _PublicationTimeline(time.perf_counter_ns())

        def observe(value: dict[str, object]) -> None:
            timeline.observe(value)
            record_phase(value)

        fixture = publish_shared_history(
            directory / "collection",
            members=len(geometry.payload_sizes),
            geometry=geometry,
            progress=observe,
            receive_workflow=lambda container: container.collection_uploads.close(),
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
                    "publication_seconds_scope": (
                        "fixture setup, publication, catalog checks and source removal"
                    ),
                    "workflow_clock_scope": (
                        "original admission through result and publication-owner receiving"
                    ),
                    "native_cache_byte_budget": _OBJECT_CACHE_BYTES,
                    "native_cache_entry_budget": _OBJECT_CACHE_ENTRIES,
                    "cache_scope": "verified Native bytes; OS caches are not cleared",
                    "verification_scope": "full ciphertext checks and Native history per pass",
                    "production_memory_qualification": False,
                    "independent_installed_recovery": False,
                },
                "publication_seconds": publication_seconds,
                "publication_workflow": timeline.verified(fixture.archive_root_sha256),
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
