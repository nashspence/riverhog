from __future__ import annotations

import hashlib
import os
import sqlite3
from contextlib import contextmanager

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    JournalResultOpening,
    assertion,
    create_journal,
    open_retained_journal_result,
    reference,
)
from test_journal_result import completed, history
from test_journal_result_opening import failed_open

CLOSE = JournalResultOpening.close


@contextmanager
def observe(source, monkeypatch):
    metrics = {bound.metric: 0 for bound in growth_binding(CLOSE).declaration.bounds}
    recording = {"active": False}
    connect = sqlite3.connect

    class Connection(sqlite3.Connection):
        def close(self):
            if recording["active"]:
                metrics["closed_databases"] += 1
            return super().close()

    def open_connection(*args, **kwargs):
        connection = connect(*args, factory=Connection, **kwargs)

        def trace(_statement):
            if recording["active"]:
                metrics["sql_statements"] += 1

        def authorize(action, table, column, *_rest):
            if recording["active"]:
                if action in {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}:
                    pytest.fail("opening cleanup attempted a Native write")
                if (
                    action == sqlite3.SQLITE_READ
                    and table in {"frames", "assertions", "journal_validation_frame"}
                    and column == "value"
                ):
                    metrics["historical_payload_reads"] += 1
                    return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_trace_callback(trace)
        connection.set_authorizer(authorize)
        return connection

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", open_connection)
        current = source.verify_current

        def checked():
            if recording["active"]:
                metrics["source_probes"] += 1
            return current()

        patch.setattr(source, "verify_current", checked)
        for name in ("read_window", "read_exact"):
            actual = getattr(source, name)

            def read(*args, _actual=actual, **kwargs):
                result = _actual(*args, **kwargs)
                if recording["active"]:
                    metrics["source_payload_octets"] += len(result)
                return result

            patch.setattr(source, name, read)
        for name in ("unlink", "remove", "rmdir", "mkdir", "rename", "replace"):
            actual = getattr(os, name)

            def mutate(*args, _actual=actual, **kwargs):
                if recording["active"]:
                    metrics["filesystem_mutations"] += 1
                return _actual(*args, **kwargs)

            patch.setattr(os, name, mutate)
        yield metrics, recording


def sample(dimensions, metrics):
    binding = growth_binding(CLOSE)
    projection = project_bindings((binding,))
    result = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": dict(metrics),
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [result])
    return result


@pytest.mark.growth_witness(
    "riverhog-retained-result-opening-close-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_result_opening.JournalResultOpening.close",
)
def test_actual_failed_opening_close_ports_remain_independent_of_retained_history(
    tmp_path, journal, graph, who, catalog, monkeypatch, record_property
):
    samples = []
    for ordinal, (frames, extensions) in enumerate(((1, 0), (17, 0), (1, 128))):
        raw = journal
        if extensions:
            enriched = {
                **graph,
                "extensions": [
                    assertion(
                        "extension",
                        who,
                        subject=reference(graph["artifacts"][0]["id"], "artifact"),
                        property="urn:test:opening-close",
                        value={"type": "text", "value": str(index)},
                    )
                    for index in range(extensions)
                ],
            }
            raw = create_journal(enriched, recorded_by_agent_id=who, catalog=catalog)
        raw = history(raw, frames, catalog)
        root = tmp_path / str(ordinal)
        root.mkdir()
        path, source, confirmed, _, _expected = completed(root, raw, catalog)
        connection = sqlite3.connect(path / "snapshot.sqlite3")
        try:
            indexed = connection.execute("SELECT count(*) FROM assertions").fetchone()[0]
        finally:
            connection.close()
        dimensions = {"frozen_frames": confirmed.accepted_frames, "indexed_assertions": indexed}
        for kind in ("success", "before", "after", "already-closed", "transferred"):
            results, openings = [], []
            try:
                with observe(source, monkeypatch) as (metrics, recording):
                    if kind == "transferred":
                        result = open_retained_journal_result(
                            path,
                            source,
                            confirmed=confirmed,
                            catalog=catalog,
                            on_storage_open=openings.append,
                        )
                        results.append(result)
                        opening = openings[0]
                        recording["active"] = True
                        try:
                            CLOSE(opening)
                        finally:
                            recording["active"] = False
                        assert metrics["closed_databases"] == 0
                        assert result.journal_sha256 == source.binding.source_sha256
                    else:
                        with failed_open(
                            path,
                            source,
                            confirmed,
                            catalog,
                            monkeypatch,
                            boundary="prefix",
                            after=False,
                        ) as (opening, fault, releases):
                            if kind in {"before", "after"}:
                                fault.after = kind == "after"
                                fault.failures = 1
                            elif kind == "already-closed":
                                opening.close()
                            recording["active"] = True
                            try:
                                if kind in {"before", "after"}:
                                    with pytest.raises(OSError, match="close unconfirmed"):
                                        CLOSE(opening)
                                else:
                                    CLOSE(opening)
                            finally:
                                recording["active"] = False
                            assert not releases
                            assert metrics["closed_databases"] == int(kind in {"success", "after"})
                            # The caller's release resources have never transferred.
                            metrics["release_callbacks"] += len(releases)
                    samples.append(sample(dimensions, metrics))
            finally:
                for result in results:
                    result.close()
                for opening in openings:
                    opening.close()
    record_property("growth_samples", samples)
