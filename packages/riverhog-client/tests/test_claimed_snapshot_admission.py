from __future__ import annotations

import shutil
from contextlib import contextmanager

import pytest
from riverhog_archive_contracts import BOUND_HISTORY_EXTENT
from riverhog_client.processing import provenance as owner
from riverhog_provenance import external_reference
from test_claimed_history_admission import api as api
from test_claimed_history_admission import reader

from tests.support.sqlite_admission import Capacity, Population


def view(api, population, handles, parents):
    return reader(
        api,
        population,
        parents,
        provenance_connection_factory=population,
        on_provenance_storage_open=handles.append,
    ).provenance(api.artifact)


@pytest.mark.parametrize("kind", ["bound", "foreign", "closure"])
def test_claimed_snapshot_admission_preserves_exact_owned_results(api, kind):
    population, handles, parents = Population(limit=3), [], []
    selected = view(api, population, handles, parents)
    if kind == "bound":
        with selected.bound_summary() as summary:
            assert summary.anchor == api.binding.journal.model_dump(mode="json")
            assert summary._store is handles[1] and population.active == 1
    elif kind == "foreign":
        reference = external_reference(api.summary, api.summary.states[0]["id"])
        assert selected.resolve_external_reference(reference) == {
            key: value for key, value in reference.items() if key != "scope"
        }
    else:
        with selected.history_closure(extent=BOUND_HISTORY_EXTENT) as closure:
            (anchor,) = closure.journal_anchors()
            summary = closure.summary_at(anchor)
            assert summary.anchor == api.binding.journal.model_dump(mode="json")
            assert summary._store is handles[-1]
            assert closure.membership_image(max_bytes=1024 * 1024) is not None
            assert len(handles) == 4 and population.peak == 3
        assert population.active == 1 and summary.anchor == api.binding.journal.model_dump(
            mode="json"
        )
        summary.close()
    assert population.active == 0
    assert len(parents) == int(kind == "closure")
    assert len(population.connections) == (3 if kind == "closure" else 1)
    assert all(connection.close_calls == 1 for connection in population.connections)
    assert not any(path.parent.exists() for path, _ in population.attempts)


@pytest.mark.parametrize("unreturned", [False, True])
@pytest.mark.parametrize("kind", ["bound", "foreign", "closure"])
def test_claimed_snapshot_refusal_precedes_chunk_iteration_and_keeps_factory_custody(
    api, monkeypatch, kind, unreturned
):
    population, handles, parents, reads = Population(), [], [], []
    history = Population()
    selected = reader(
        api,
        history,
        parents,
        provenance_connection_factory=population,
        on_provenance_storage_open=handles.append,
    ).provenance(api.artifact)
    stream = api.stream_collection_provenance_journal

    @contextmanager
    def measured(*args, **options):
        with stream(*args, **options) as chunks:

            def recorded():
                for chunk in chunks:
                    reads.append(len(chunk))
                    yield chunk

            yield recorded()

    monkeypatch.setattr(api, "stream_collection_provenance_journal", measured)
    population.refuse, population.unreturned = not unreturned, unreturned
    with pytest.raises(OSError if unreturned else Capacity):
        if kind == "bound":
            selected.bound_summary()
        elif kind == "foreign":
            selected.resolve_external_reference(
                external_reference(api.summary, api.summary.states[0]["id"])
            )
        else:
            with selected.history_closure(extent=BOUND_HISTORY_EXTENT):
                pytest.fail("refused snapshot returned")
    assert not reads and not handles and population.active == int(unreturned)
    assert history.active == 0
    assert not any(path.parent.exists() for path, _ in history.attempts)
    if unreturned:
        assert population.connections[0].close_calls == 0
        population.connections[0].close()
    shutil.rmtree(population.attempts[0][0].parent)
    assert population.active == 0


@pytest.mark.parametrize("after", [None, False, True])
def test_claimed_bound_summary_retains_ownership_through_final_root_fence_failure(
    api, monkeypatch, after
):
    population, handles, parents = Population(), [], []
    selected = view(api, population, handles, parents)
    validate = owner.validate_journal_chunks

    def fenced(chunks, **options):
        summary = validate(chunks, **options)

        def revoked():
            raise PermissionError("final root fence revoked")

        selected.verify_root = revoked
        if after is not None:
            population.connections[0].close_failures = 1
            population.connections[0].close_after = after
        return summary

    monkeypatch.setattr(owner, "validate_journal_chunks", fenced)
    with pytest.raises(PermissionError if after is None else OSError):
        selected.bound_summary()
    assert len(handles) == 2 and len(population.connections) == 1
    assert population.active == int(after is not None)
    assert population.connections[0].close_calls == 1
    if after is not None:
        assert population.attempts[0][0].is_file()
        handles[-1].close()
    assert population.active == 0 and not population.attempts[0][0].parent.exists()
