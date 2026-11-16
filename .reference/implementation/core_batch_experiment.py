"""Exercise retained Core effect owners with real encrypted filesystem writes.

This reference calls the unchanged Core owners and maintained test construction.
The individual comparator settles each already returned effect separately and
applies the format checkpoint only with the last effect. The batch case uses
ArchiveWriteAdmission.apply_reports unchanged. Neither case batches admission,
releases an unknown execution, or changes acknowledgement/retirement semantics.

Run from the checkout with its locked Core/SDK test environment and source roots:
PYTHONPATH=.:some-implementations/riverhog/storage/filesystem/src:\
packages/riverhog-storage-adapter-asgi-support/src python \
.reference/implementation/core_batch_experiment.py --output /tmp/core-batch.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from time import perf_counter_ns
from typing import Any, Literal

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from sqlalchemy import event, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from tests.unit.test_archive_upload_writes import _actor, _open, _unit

Mode = Literal["individual", "batch"]
Kind = Literal["pack", "segment"]


@dataclass
class TransactionCounts:
    begins: int = 0
    early_writer_attempts: int = 0
    commit_attempts: int = 0
    successful_session_commits: int = 0
    rollback_calls: int = 0
    sql_reads: int = 0
    sql_mutations: int = 0


class CatalogTrace:
    """Count actual SQLAlchemy events; physical synchronization is unobserved.

    SQLAlchemy's engine commit event precedes the DBAPI call, so it is explicitly
    an attempt. Session.after_commit supplies the distinct successful return.
    Read-only and fixture inspection transactions remain visible by stage.
    """

    def __init__(self) -> None:
        self.stage = "fixture_setup"
        self.counts: dict[str, TransactionCounts] = {}
        self.elapsed_ns: Counter[str] = Counter()

    def _current(self) -> TransactionCounts:
        return self.counts.setdefault(self.stage, TransactionCounts())

    def _begin(self, connection: Any) -> None:
        self._current().begins += 1

    def _commit(self, connection: Any) -> None:
        self._current().commit_attempts += 1

    def _committed(self, session: Session) -> None:
        self._current().successful_session_commits += 1

    def _rollback(self, connection: Any) -> None:
        self._current().rollback_calls += 1

    def _statement(self, connection: Any, cursor: Any, statement: str, *args: Any) -> None:
        command = statement.lstrip().upper()
        counts = self._current()
        if command.startswith("BEGIN IMMEDIATE"):
            counts.early_writer_attempts += 1
        elif command.startswith("SELECT"):
            counts.sql_reads += 1
        elif command.startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER")):
            counts.sql_mutations += 1

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        previous, self.stage = self.stage, name
        started = perf_counter_ns()
        try:
            yield
        finally:
            self.elapsed_ns[name] += perf_counter_ns() - started
            self.stage = previous

    def __enter__(self) -> CatalogTrace:
        self._listeners = (
            (Engine, "begin", self._begin),
            (Engine, "commit", self._commit),
            (Engine, "rollback", self._rollback),
            (Engine, "before_cursor_execute", self._statement),
            (Session, "after_commit", self._committed),
        )
        for target, name, callback in self._listeners:
            event.listen(target, name, callback)
        return self

    def __exit__(self, *args: Any) -> None:
        for target, name, callback in reversed(self._listeners):
            event.remove(target, name, callback)

    def snapshot(self) -> dict[str, Any]:
        total = TransactionCounts()
        for counts in self.counts.values():
            for field, value in asdict(counts).items():
                setattr(total, field, getattr(total, field) + value)
        return {
            "by_stage": {name: asdict(counts) for name, counts in sorted(self.counts.items())},
            "totals": asdict(total),
            "stage_elapsed_ns": dict(sorted(self.elapsed_ns.items())),
            "stage_timing_semantics": "inclusive nested phases; do not sum phase elapsed times",
            "physical_sync_calls": None,
            "sync_observation": "not instrumented; commit events are not synchronization counts",
        }


def stored(actor: Any) -> tuple[str | None, list[tuple[str, str, bool]]]:
    """Read exact checkpoint and retained effect identities for fault assertions."""
    case = actor.case
    with session_scope(case.factory) as session:
        row = session.get(CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id))
        assert row is not None
        effects = session.scalars(
            select(ArchiveWriteEffectRecord).where(
                ArchiveWriteEffectRecord.collection_id == case.collection_id
            )
        )
        return row.checkpoint_json, [(item.id, item.state, item.unresolved) for item in effects]


def run_case(directory: Path, *, mode: Mode, kind: Kind, plaintext_bytes: int) -> dict[str, Any]:
    """One disposable real-owner case, including visible setup and cleanup costs."""
    if mode not in {"individual", "batch"} or kind not in {"pack", "segment"}:
        raise ValueError("unknown reference experiment selection")
    if type(plaintext_bytes) is not int or plaintext_bytes < 65536:
        raise ValueError("the source witness requires a complete nontrivial encryption chunk")
    directory.mkdir(parents=True)
    content = b"x" * plaintext_bytes
    started = perf_counter_ns()
    logical_batches: list[dict[str, Any]] = []
    apply_calls: Counter[str] = Counter()
    provider_bytes = 0
    calls: Counter[str] = Counter()
    with CatalogTrace() as trace, pytest.MonkeyPatch.context() as patch:
        original_prepare = ArchiveWriteAdmission.prepare

        def prepare(owner: Any, *args: Any, **kwargs: Any) -> Any:
            with trace.phase("preparation"):
                return original_prepare(owner, *args, **kwargs)

        patch.setattr(ArchiveWriteAdmission, "prepare", prepare)
        with _actor(
            directory,
            patch,
            kind,
            content=content,
            unit_plaintext_bytes=plaintext_bytes,
        ) as actor:
            arm = actor.admission.arm
            execute = actor.executor.execute_write
            report = actor.admission.apply_reports
            retire = actor.admission.acknowledge_report

            def admit(*args: Any, **kwargs: Any) -> Any:
                with trace.phase("effect_admission"):
                    return arm(*args, **kwargs)

            def dispatch(invocation: Any, *, content: bytes = b"") -> Any:
                nonlocal provider_bytes
                calls[invocation.operation.kind] += 1
                provider_bytes += len(content)
                # The maintained fixture independently acquires a writer here,
                # proving the request was committed before provider dispatch.
                with trace.phase("provider_and_fixture_admission_check"):
                    return execute(invocation, content=content)

            def receive(reports: tuple[Any, ...], *, apply_result: Any) -> None:
                outcome = str(reports[-1].result["outcome"])
                logical_batches.append(
                    {
                        "outcome": outcome,
                        "effects": len(reports),
                        "distinct_call_ids": len({item.ticket.call_id for item in reports}),
                        "distinct_checkpoint_hashes": len(
                            {item.result["checkpoint_sha256"] for item in reports}
                        ),
                    }
                )

                def apply(*args: Any) -> None:
                    apply_calls[outcome] += 1
                    apply_result(*args)

                with trace.phase("result_" + outcome):
                    if mode == "batch":
                        report(reports, apply_result=apply)
                    else:
                        # All returned effects were separately prearmed. Earlier
                        # durable outcomes cannot publish the logical checkpoint.
                        for index, item in enumerate(reports):
                            report(
                                (item,),
                                apply_result=apply
                                if index == len(reports) - 1
                                else lambda *a: None,
                            )

            def acknowledge(*args: Any, **kwargs: Any) -> None:
                with trace.phase("report_receiving_and_retirement"):
                    retire(*args, **kwargs)

            patch.setattr(actor.admission, "arm", admit)
            patch.setattr(actor.executor, "execute_write", dispatch)
            patch.setattr(actor.admission, "apply_reports", receive)
            patch.setattr(actor.admission, "acknowledge_report", acknowledge)
            with trace.phase("format_open"):
                opened = _open(actor)
            with trace.phase("format_logical_unit"):
                completed = _unit(actor, opened)
            assert completed.completed is not None
            with trace.phase("verification"):
                checkpoint, effects = stored(actor)
            assert checkpoint == completed.to_json() and effects == []
            assert not actor.journal.has_pending
            call_ids = [invocation.call.call_id for invocation in actor.seen]
            assert len(set(call_ids)) == len(call_ids)
            # Cleanup is still performed by the original service/adapter owners.
            trace.stage = "fixture_cleanup"
        observations = trace.snapshot()
    return {
        "mode": mode,
        "kind": kind,
        "plaintext_bytes": plaintext_bytes,
        "plaintext_sha256": hashlib.sha256(content).hexdigest(),
        "provider_request_bytes": provider_bytes,
        "provider_calls": dict(sorted(calls.items())),
        "distinct_provider_calls": len(call_ids),
        "logical_report_batches": logical_batches,
        "format_result_apply_calls": dict(sorted(apply_calls.items())),
        "retained_effects_after_original_receiving": 0,
        "final_checkpoint_completed": True,
        "elapsed_ns_including_fixture": perf_counter_ns() - started,
        "transactions": observations,
    }


def run_comparison(directory: Path) -> dict[str, Any]:
    rows = [
        run_case(directory / f"{kind}-{size}-{mode}", mode=mode, kind=kind, plaintext_bytes=size)
        for kind in ("segment", "pack")
        for size in (65536, 131072, 262144)
        for mode in ("individual", "batch")
    ]
    return {
        "schema": "riverhog-reference-core-effect-batch-1",
        "runtime": {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version},
        "scope": "unchanged actual Core owner; direct filesystem fixture; source dependency paths",
        "comparison": "controlled per-report comparator versus existing atomic report batch",
        "limits": [
            "not an installed adapter, authenticated API, F1, platform or provider qualification",
            "only result settlement is grouped; all admissions and original retirements remain",
            "engine commit attempts and Session successful commits are not physical sync counts",
            "timing includes fixture work and is a machine-dependent observation",
        ],
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="riverhog-core-batch-") as temporary:
        result = run_comparison(Path(temporary))
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
