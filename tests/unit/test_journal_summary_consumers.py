from __future__ import annotations

import hashlib

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.canonical_discovery_rebuild import _ArchiveCorpus
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    JournalSummary,
    assertion,
    create_journal,
    journal_store,
    reference,
    validate_journal,
)


def source(text_octets=0, *, journal_id=None):
    observed = BoundedSourceObserver().observe(BytesSource(b"native summary"))
    graph = observed.graph_fragment()
    if text_octets:
        graph["extensions"] = [
            assertion(
                "extension",
                observed.observer_agent_id,
                subject=reference(graph["artifacts"][0]["id"], "artifact"),
                property="urn:test:summary-scope",
                value={"type": "text", "value": "x" * text_octets},
            )
        ]
    return create_journal(
        graph, recorded_by_agent_id=observed.observer_agent_id, journal_id=journal_id
    )


def test_native_archive_corpus_eviction_releases_only_its_owned_lease():
    raw = [source() for _ in range(3)]
    documents = {}
    for content in raw:
        with validate_journal(content) as summary:
            documents[summary.journal_id] = content
    identities = list(documents)

    class Reader:
        def iter_journal_range(self, identity):
            yield documents[identity]

    with _ArchiveCorpus(Reader()) as corpus:
        with corpus[identities[0]].borrow() as selected:
            projection = selected._store._storage
            expected = selected.materialize()
            corpus[identities[1]]
            corpus[identities[2]]
            assert identities[0] not in corpus.cache
            assert selected.materialize() == expected and projection.directory.exists()
        assert not projection.directory.exists()
        remaining = [value._store._storage.directory for value in corpus.cache.values()]
    assert not any(path.exists() for path in remaining)


@pytest.mark.growth_witness(
    "riverhog-frozen-summary-borrow-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal.JournalSummary.borrow",
)
def test_native_summary_borrow_records_actual_work(monkeypatch, record_property):
    binding = growth_binding(JournalSummary.borrow)
    projection = project_bindings((binding,))
    declaration = projection["declarations"][binding.declaration.id]
    samples = []
    connect = journal_store.sqlite3.connect
    for extra in (0, 4096, 65536):
        raw = source(extra)
        with validate_journal(raw) as summary:
            connection = summary._store.db
            statements = []
            opened = []

            def observed_connect(*args, observations=opened, **kwargs):
                observations.append(True)
                return connect(*args, **kwargs)

            connection.set_trace_callback(statements.append)
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(journal_store.sqlite3, "connect", observed_connect)
                    borrowed = summary.borrow()
                counts = {
                    "returned_handles": int(borrowed is not summary),
                    "sql_statements": len(statements),
                    "opened_databases": len(opened),
                }
            finally:
                connection.set_trace_callback(None)
            with borrowed:
                assert borrowed._store is not summary._store
                assert borrowed._store._storage is summary._store._storage
                assert borrowed.anchor == summary.anchor
                assert borrowed.journal_sha256 == hashlib.sha256(raw).hexdigest()
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": hashlib.sha256(
                        canonical_json_bytes(declaration)
                    ).hexdigest(),
                    "dimensions": {"snapshot_octets": len(raw)},
                    "metrics": counts,
                    "delegates": {},
                }
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
    record_property("growth_samples", samples)


def test_native_staged_corpus_cache_eviction_preserves_an_independent_reader(tmp_path):
    from riverhog_core.services.collection_uploads import _StagedCanonicalCorpus
    from sqlalchemy.orm import Session

    from tests.unit.test_staged_journal_range import JOURNAL, Chunk, Journal, staged_transport

    content = [source(journal_id=JOURNAL), source(), source()]
    identities = []
    for raw in content:
        with validate_journal(raw) as summary:
            identities.append(summary.journal_id)
    with staged_transport(tmp_path, 1, last_octets=len(content[0])) as (engine, _):
        with Session(engine) as session, session.begin():
            record = session.get(Journal, (1, JOURNAL))
            record.state = "sealed"
            record.sha256 = hashlib.sha256(content[0]).hexdigest()
            session.get(Chunk, (1, JOURNAL, 0)).content = content[0]
            for identity, raw in zip(identities[1:], content[1:], strict=True):
                session.add(
                    Journal(
                        collection_id=1,
                        journal_id=identity,
                        bytes=len(raw),
                        sha256=hashlib.sha256(raw).hexdigest(),
                        accepted_bytes=len(raw),
                        next_chunk_ordinal=1,
                        content_hash_state="{}",
                        state="sealed",
                    )
                )
                session.flush()
                session.add(
                    Chunk(
                        collection_id=1, journal_id=identity, ordinal=0, byte_offset=0, content=raw
                    )
                )
        with (
            Session(engine) as session,
            _StagedCanonicalCorpus(
                session,
                1,
                open_summary=lambda identity: validate_journal(content[identities.index(identity)]),
            ) as corpus,
        ):
            with corpus[identities[0]].borrow() as selected:
                directory = selected._store._storage.directory
                expected = selected.materialize()
                corpus[identities[1]]
                corpus[identities[2]]
                assert identities[0] not in corpus.cache
                assert selected.materialize() == expected and directory.exists()
                assert corpus[identities[0]].anchor == selected.anchor
                assert selected.materialize() == expected
            assert not directory.exists()
            remaining = [row._store._storage.directory for row in corpus.cache.values()]
        assert not any(path.exists() for path in remaining)
