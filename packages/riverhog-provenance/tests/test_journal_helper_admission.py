from __future__ import annotations

import gc
import shutil
from pathlib import Path

import pytest
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    ObservationRequest,
    ProvenanceValidationError,
    append_observation,
    create_journal,
    journal_store,
    recover_complete_prefix,
    validate_journal,
    validate_journal_set,
    validate_journal_set_chunks,
)
from riverhog_provenance import journal as owner

from tests.support.sqlite_admission import Capacity, Population

KINDS = ("set", "chunks", "recovery", "observation")
COUNTS = {"set": 3, "chunks": 3, "recovery": 1, "observation": 3}
STAGES = tuple((kind, stage) for kind in KINDS for stage in range(1, COUNTS[kind] + 1))


@pytest.fixture
def material(journal, observation, who, catalog, monkeypatch):
    with validate_journal(journal, catalog=catalog) as summary:
        anchor = summary.anchor
        child = create_journal(
            {"agents": summary.graph["agents"]},
            recorded_by_agent_id=who,
            forked_from=anchor,
            catalog=catalog,
        )
    measured = BoundedSourceObserver(catalog=catalog).observe(
        BytesSource(b"new exact observed content"),
        ObservationRequest(artifact=observation.artifact, occurrence=observation.occurrence),
    )
    monkeypatch.setattr(owner, "new_id", lambda: "urn:uuid:00000000-0000-4000-8000-000000000009")
    monkeypatch.setattr(owner, "utc_now", lambda: "2026-10-30T00:00:00Z")
    return {"raw": journal, "child": child, "catalog": catalog, "observation": measured}


@pytest.fixture
def paths(tmp_path, monkeypatch):
    make, paths = journal_store.mkdtemp, []

    def created(**options):
        path = Path(make(dir=tmp_path, **options))
        paths.append(path)
        return str(path)

    monkeypatch.setattr(journal_store, "mkdtemp", created)
    return paths


def forbid_default(monkeypatch):
    def denied(*_args, **_options):
        raise AssertionError("journal helper bypassed the selected factory")

    monkeypatch.setattr(journal_store.sqlite3, "connect", denied)


def invoke(kind, material, **ports):
    raw, catalog = material["raw"], material["catalog"]
    if kind == "set":
        return validate_journal_set((raw, material["child"]), catalog=catalog, **ports)
    if kind == "chunks":
        return validate_journal_set_chunks(
            (
                (value[i : i + 7] for i in range(0, len(value), 7))
                for value in (raw, material["child"])
            ),
            catalog=catalog,
            **ports,
        )
    if kind == "recovery":
        return recover_complete_prefix(raw + b'\x1e{"torn":', catalog=catalog, **ports)
    assert kind == "observation"
    return append_observation(raw, material["observation"], catalog=catalog, **ports)


def close_result(kind, result):
    if kind == "recovery":
        result.prefix_validation.close()
    elif kind != "observation":
        result.close()


def exact_result(kind, result):
    if kind == "recovery":
        return result.complete_prefix, result.incomplete_tail, result.prefix_validation.anchor
    if kind == "observation":
        return result
    return tuple(journal.anchor for journal in result.journals), result.findings


@pytest.mark.parametrize("kind", KINDS)
def test_journal_helpers_compose_real_admission_and_preserve_original_results(
    material,
    paths,
    monkeypatch,
    kind,
):
    result = invoke(kind, material)
    expected = exact_result(kind, result)
    close_result(kind, result)
    forbid_default(monkeypatch)
    population, handles = Population(limit=3), []
    result = invoke(kind, material, connection_factory=population, on_storage_open=handles.append)
    assert exact_result(kind, result) == expected
    assert len(population.connections) == COUNTS[kind] and len(handles) == 2 * COUNTS[kind]
    assert population.peak == (1 if kind == "recovery" else 3)
    assert population.active == (2 if kind in ("set", "chunks") else 1 if kind == "recovery" else 0)
    close_result(kind, result)
    assert population.active == 0 and all(db.close_calls == 1 for db in population.connections)
    assert all(not path.exists() for path in paths)


@pytest.mark.parametrize("kind,stage", STAGES)
def test_journal_helper_refusal_releases_returned_owners_and_preserves_factory_files(
    material,
    paths,
    monkeypatch,
    kind,
    stage,
):
    forbid_default(monkeypatch)
    population, handles = Population(limit=3), []

    def admitted(database, **options):
        population.refuse = len(population.attempts) == stage - 1
        return population(database, **options)

    with pytest.raises(Capacity):
        invoke(kind, material, connection_factory=admitted, on_storage_open=handles.append)
    assert population.active == 0 and len(population.connections) == stage - 1
    assert len(handles) == 2 * (stage - 1)
    assert all(not path.exists() for path in paths[:-1])
    assert paths[-1].is_dir() and not list(paths[-1].iterdir())
    shutil.rmtree(paths[-1])


@pytest.mark.parametrize("kind,stage", STAGES)
@pytest.mark.parametrize("after", [False, True])
def test_journal_helper_uncertain_close_retains_actual_reader_without_implicit_retry(
    material,
    paths,
    monkeypatch,
    kind,
    stage,
    after,
):
    forbid_default(monkeypatch)
    population, handles = Population(limit=3), []

    def retained(store):
        handles.append(store)
        if len(handles) == 2 * stage:
            store._storage.connection.close_failures = 1
            store._storage.connection.close_after = after

    with pytest.raises(OSError, match="close unconfirmed"):
        result = invoke(kind, material, connection_factory=population, on_storage_open=retained)
        close_result(kind, result)
    gc.collect()
    db = population.connections[stage - 1]
    assert db.close_calls == 1 and not db.released
    assert not handles[2 * stage - 1]._lease.closed and paths[stage - 1].is_dir()
    assert population.active >= 1
    # A set whose first close failed still owns its later, unclosed reader too.
    for handle in handles:
        if not handle._lease.closed:
            handle.close()
    assert db.close_calls == 2 and population.active == 0
    assert all(not path.exists() for path in paths)


@pytest.mark.parametrize("stream", [False, True])
def test_journal_set_duplicate_prefix_releases_superseded_reader_capacity(
    material,
    paths,
    monkeypatch,
    stream,
):
    forbid_default(monkeypatch)
    population, handles = Population(limit=2), []
    raw = material["raw"]
    validate = validate_journal_set_chunks if stream else validate_journal_set
    values = ((raw,), (raw,)) if stream else (raw, raw)
    result = validate(
        values,
        catalog=material["catalog"],
        connection_factory=population,
        on_storage_open=handles.append,
    )
    assert len(result.journals) == 1 and len(population.connections) == 2 and len(handles) == 4
    assert population.active == 1 and population.peak == 2
    assert population.connections[0].released and not population.connections[1].released
    result.close()
    assert population.active == 0 and all(not path.exists() for path in paths)


def test_admitted_prefix_recovery_rejects_invalid_tail_and_closes_real_reader(
    material,
    paths,
    monkeypatch,
):
    forbid_default(monkeypatch)
    population, handles = Population(limit=1), []
    with pytest.raises(ProvenanceValidationError, match="incomplete RFC 7464 frame"):
        recover_complete_prefix(
            material["raw"] + b"invalid-tail",
            catalog=material["catalog"],
            connection_factory=population,
            on_storage_open=handles.append,
        )
    assert len(population.connections) == 1 and len(handles) == 2 and population.active == 0
    assert all(not path.exists() for path in paths)
