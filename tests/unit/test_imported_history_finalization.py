from __future__ import annotations

import hashlib
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import replace
from time import perf_counter, thread_time
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    ArchiveProvenanceIdentity,
    CollectionArchiveManifest,
    CollectionArtifactSetIdentity,
    HistoryJournalAnchor,
    MemberHistoryBuilder,
    MemberHistoryImport,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    MemberHistoryStore,
    ProvenanceRootDocument,
    ProvenanceRootIdentity,
    SourceMemberHistoryBindingProof,
    binding_tree_commitment,
    provenance_structure_identity,
    provenance_structure_object_id,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalChunkRecord as Chunk
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_core.services import collection_uploads as service_owner
from riverhog_core.services import upload_imported_history as import_owner
from riverhog_core.services import upload_journal_result as result_owner
from riverhog_protocol import MemberHistoryBindingBatchDocument
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_protocol.errors import Conflict
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    CanonicalCorpusValidator,
    JournalValidationSession,
    MemberHistoryClosure,
    ProvenanceValidationError,
    append_correction,
    assertion,
    assertion_reference,
    create_journal,
    external_reference,
    journal_store,
    new_id,
    parse_journal,
    reference,
    selected_delivery_occurrence,
    validate_journal,
)
from riverhog_provenance import history as history_owner
from sqlalchemy import event, select, update

from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_uploads import _bind_primary, _upload_provenance_journal
from tests.unit.test_finalization_claim_readiness import _seal
from tests.unit.test_selected_upload_journal_prefix import _advance_member, _finish, _owner, _state


def _source_material(text_octets=0, *, journal_id=None):
    catalog = service_owner.admission_provenance_catalog()
    payload = b"exact imported primary"
    artifact_id = "f" * 64
    observed = BoundedSourceObserver(catalog=catalog).observe(BytesSource(payload))
    graph, who = observed.graph_fragment(), observed.observer_agent_id
    context = assertion("context", who, kind="delivery")
    association = assertion(
        "delivery_association",
        who,
        delivery_context_id=context["id"],
        slot={"kind": "text", "text": artifact_id},
        role=COLLECTION_MEMBER_ROLE,
        state=reference(observed.state_id, "state"),
        verification_observation_id=next(
            row["id"] for row in graph["descriptions"] if row["type"] == "observation"
        ),
    )
    graph["contexts"] = [*graph.get("contexts", ()), context]
    graph["delivery_associations"] = [association]

    def documentary(value):
        return assertion(
            "extension",
            who,
            subject=reference(graph["artifacts"][0]["id"], "artifact"),
            property="urn:test:imported-primary-documentary",
            value={"type": "text", "value": value},
        )

    graph["extensions"] = [documentary("x" * text_octets)]
    prefix = create_journal(graph, recorded_by_agent_id=who, catalog=catalog, journal_id=journal_id)
    with validate_journal(prefix, catalog=catalog) as summary:
        primary = MemberHistoryPrimary(
            HistoryJournalAnchor.from_mapping(summary.anchor), association["id"]
        )
        state = external_reference(summary, observed.state_id)
        correction_target = assertion_reference(summary, association["assertion_id"])
    tail_text = "retained documentary correction"
    raw = append_correction(
        prefix,
        [correction_target],
        assertions={"extensions": [documentary(tail_text)]},
        reason="delivery withdrawn after its historical selection",
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    with validate_journal(raw, catalog=catalog) as summary:
        full_anchor = HistoryJournalAnchor.from_mapping(summary.anchor)
        assert association["id"] not in summary.graph_validation.objects
    with MemberHistoryBuilder(
        artifact_id=artifact_id,
        bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        primary=primary,
    ) as builder:
        builder.add_root(MemberHistoryRoot(full_anchor, "retained"))
        binding, descriptor = builder.seal()
        objects = {
            provenance_structure_identity(content).object_id: content
            for content in builder.objects()
        }
    provenance = ProvenanceRootDocument(
        archive_generation="1" * 64,
        artifact_set_sha256="2" * 64,
        delivery_context_id=context["id"],
        binding_count=1,
        binding_tree_sha256=binding_tree_commitment((binding,)).root_sha256,
        journal_count=1,
        ordered_volume_sha256="3" * 64,
    )
    archive = CollectionArchiveManifest(
        archive_generation=provenance.archive_generation,
        artifact_set=CollectionArtifactSetIdentity(1, len(payload), provenance.artifact_set_sha256),
        ordered_volume_sha256="4" * 64,
        provenance=ArchiveProvenanceIdentity(
            provenance.identity,
            ProvenanceRootIdentity(
                id="provenance-root",
                kind="provenance-root",
                path="provenance/root.json.age",
                plaintext_bytes=len(provenance.to_json_bytes()),
                sha256=provenance.identity,
                stored_bytes=1234,
                stored_sha256="5" * 64,
            ),
        ),
    )
    proof = SourceMemberHistoryBindingProof(
        "6" * 64, 7, archive.to_json_bytes(), provenance.to_json_bytes(), binding, 0, ()
    )
    objects[provenance_structure_identity(proof.to_json_bytes()).object_id] = proof.to_json_bytes()
    imported = MemberHistoryImport(
        proof.source_identity,
        proof.collection_id,
        hashlib.sha256(proof.archive_root).hexdigest(),
        provenance.artifact_set_sha256,
        descriptor.artifact_id,
        descriptor.identity,
        proof.identity,
        RETAINED_HISTORY_EXTENT,
        state,
    )
    return SimpleNamespace(
        raw=raw,
        prefix=prefix,
        full_anchor=full_anchor,
        primary=primary,
        descriptor=descriptor,
        binding=binding,
        imported=imported,
        objects=objects,
        tail_text=tail_text,
        catalog=catalog,
        graph=graph,
        who=who,
    )


@contextmanager
def _staged_imports(tmp_path, monkeypatch, material, *, fanout=4, members=4):
    monkeypatch.setattr(service_owner, "_FINALIZATION_FILE_BATCH", 1)
    with _case(tmp_path, "segment", artifact_count=members) as case:
        primaries = [
            _bind_primary(case.service, case.collection_id, format(i + 1, "064x"), case.content)
            for i in range(members)
        ]
        _upload_provenance_journal(
            case.service,
            case.collection_id,
            material.primary.journal.journal_id,
            material.raw,
            hashlib.sha256(material.raw).hexdigest(),
        )
        for content in material.objects.values():
            case.service.stage_history_structure(case.collection_id, content)
        bindings = []
        for index, produced in enumerate(primaries):
            with MemberHistoryBuilder(
                artifact_id=produced.binding.artifact_id,
                bytes=len(case.content),
                sha256=hashlib.sha256(case.content).hexdigest(),
                primary=MemberHistoryPrimary.from_mapping(
                    {
                        "journal": produced.binding.journal.model_dump(mode="json"),
                        "delivery_association_id": produced.binding.delivery_association_id,
                    }
                ),
            ) as builder:
                if index < fanout:
                    builder.add_import(material.imported)
                binding, _ = builder.seal()
                bindings.append(binding)
                for content in builder.objects():
                    case.service.stage_history_structure(case.collection_id, content)
                case.service.bind_member_histories(
                    case.collection_id,
                    MemberHistoryBindingBatchDocument.model_validate(
                        {"bindings": [binding.to_mapping()]}
                    ),
                )
        _seal(case)
        claim = case.service._claim_due_finalization()
        assert claim is not None
        for _ in range(members + 2):
            if not case.service._stage_next_member_histories(case.collection_id):
                break
        else:
            pytest.fail("bounded imported member history staging did not terminate")
        case.claim, case.members, case.fanout = claim, members, fanout
        case.primaries, case.histories, case.material = primaries, bindings, material
        yield case


def _octets(values):
    return sum(
        0 if value is None else len(value) if isinstance(value, bytes) else len(str(value).encode())
        for value in values
    )


@contextmanager
def _observe_terminal(case, monkeypatch):
    observed = SimpleNamespace(
        reads=defaultdict(list),
        validations=[],
        advances=0,
        imports=[],
        corpus_sources=[],
        closures=[],
        corpora=[],
        summaries=[],
        resolutions=[],
        native_openings=0,
        raw_admissions=[],
        raw_current_checks=[],
        catalog_statements=0,
        catalog_other_statements=0,
    )
    active_validation, active_import, active_source, active_raw_admission = [], [], [], []
    original_read = service_owner._iter_upload_journal_chunks
    original_advance = JournalValidationSession.advance
    original_import = import_owner.selected_upload_imported_primary_state
    original_source = CanonicalCorpusValidator._add_source
    original_metadata = CanonicalCorpusValidator._source_read
    original_frame = journal_store._Frames.__getitem__
    original_closure = service_owner._staged_history_closure
    original_open = result_owner.open_upload_journal_result
    original_prepare = result_owner.prepare_upload_journal_result
    original_current = result_owner.SealedJournalResultAdmission.verify_current

    def read(*args, **options):
        current = {
            "end": options.get("through_bytes"),
            "octets": 0,
            "exhausted": False,
            "received": False,
        }
        observed.reads[args[1].journal_id].append(current)
        original = original_read(*args, **options)
        try:
            for chunk in original:
                current["octets"] += len(chunk)
                yield chunk
            current["exhausted"] = True
        finally:
            original.close()
            current["received"] = True

    def advance(self, *args, **options):
        observed.advances += 1
        if active_validation:
            active_validation[-1]["native_advances"] += 1
        return original_advance(self, *args, **options)

    def validation(actual, owner):
        def checked(chunks, **options):
            current = {
                "owner": owner,
                "anchor": options.get("expected_anchor"),
                "octets": 0,
                "native_advances": 0,
            }
            observed.validations.append(current)

            def measured():
                for chunk in chunks:
                    current["octets"] += len(chunk)
                    yield chunk

            active_validation.append(current)
            try:
                result = actual(measured(), **options)
                current["journal_id"] = result.journal_id
                observed.summaries.append(result)
                return result
            finally:
                active_validation.pop()

        return checked

    def imported(*args, **options):
        current = {"sql_statements": 0, "native_metadata_queries": 0, "metadata_octets": 0}
        observed.imports.append(current)
        active_import.append(current)
        try:
            return original_import(*args, **options)
        finally:
            active_import.pop()

    def sql(_connection, _cursor, _statement, _parameters, _context, _many):
        observed.catalog_statements += 1
        if active_import:
            active_import[-1]["sql_statements"] += 1
        elif active_raw_admission:
            active_raw_admission[-1]["sql_statements"] += 1
        else:
            observed.catalog_other_statements += 1

    def prepare(*args, **options):
        if active_import:
            return original_prepare(*args, **options)
        current = {"journal_id": args[2], "sql_statements": 0}
        observed.raw_admissions.append(current)
        active_raw_admission.append(current)
        try:
            return original_prepare(*args, **options)
        finally:
            active_raw_admission.pop()

    def verify_current(admission, session):
        if active_import:
            return original_current(admission, session)
        current = {"journal_id": admission.binding.journal_id, "sql_statements": 0}
        observed.raw_current_checks.append(current)
        active_raw_admission.append(current)
        try:
            return original_current(admission, session)
        finally:
            active_raw_admission.pop()

    def metadata(self, statement, parameters=()):
        result = original_metadata(self, statement, parameters)
        if active_import:
            active_import[-1]["native_metadata_queries"] += 1
            active_import[-1]["metadata_octets"] += _octets(parameters)
            if result is not None:
                active_import[-1]["metadata_octets"] += _octets(result)
        return result

    def source(self, summary, **options):
        current = {"journal_id": summary.journal_id, "frames": 0, "octets": 0}
        observed.corpus_sources.append(current)
        active_source.append(current)
        try:
            return original_source(self, summary, **options)
        finally:
            active_source.pop()

    def frame(self, index):
        result = original_frame(self, index)
        if active_source and isinstance(index, int):
            active_source[-1]["frames"] += 1
            active_source[-1]["octets"] += len(result.encoded)
        return result

    def closure(*args, **options):
        result = original_closure(*args, **options)
        observed.closures.append(result)
        actual_resolve = result.resolve

        def resolve(binding, *, extent):
            observed.resolutions.append((binding, extent))
            return actual_resolve(binding, extent=extent)

        result.resolve = resolve
        return result

    def corpus():
        result = CanonicalCorpusValidator()
        observed.corpora.append(result)
        return result

    def opened(*args, **options):
        observed.native_openings += 1
        return original_open(*args, **options)

    def forbidden_open(*_args, **_options):
        observed.native_openings += 1
        pytest.fail("terminal imported-primary facts opened another Native result or lease")

    engine = case.factory.kw["bind"]
    wall, cpu = perf_counter(), thread_time()
    with monkeypatch.context() as patch:
        patch.setattr(service_owner, "_iter_upload_journal_chunks", read)
        patch.setattr(JournalValidationSession, "advance", advance)
        patch.setattr(import_owner, "selected_upload_imported_primary_state", imported)
        patch.setattr(CanonicalCorpusValidator, "_add_source", source)
        patch.setattr(CanonicalCorpusValidator, "_source_read", metadata)
        patch.setattr(journal_store._Frames, "__getitem__", frame)
        patch.setattr(service_owner, "_staged_history_closure", closure)
        patch.setattr(service_owner, "CanonicalCorpusValidator", corpus)
        patch.setattr(result_owner, "open_upload_journal_result", opened)
        patch.setattr(result_owner, "prepare_upload_journal_result", prepare)
        patch.setattr(import_owner, "prepare_upload_journal_result", prepare)
        patch.setattr(result_owner.SealedJournalResultAdmission, "verify_current", verify_current)
        patch.setattr(result_owner, "open_retained_journal_result", forbidden_open)
        patch.setattr(result_owner, "lease_journal_validation_workspace", forbidden_open)
        for module, label in ((service_owner, "terminal"), (history_owner, "history")):
            patch.setattr(
                module, "validate_journal_chunks", validation(module.validate_journal_chunks, label)
            )
        event.listen(engine, "before_cursor_execute", sql)
        try:
            yield observed
        finally:
            event.remove(engine, "before_cursor_execute", sql)
            observed.elapsed_seconds = perf_counter() - wall
            observed.thread_cpu_seconds = thread_time() - cpu


def _terminal(case):
    owner = _owner(case)
    assert owner.advance()
    assert not owner.has_pending and not owner.result_resources.pending
    return owner


def _prepare_terminal(case):
    for _ in range(case.members):
        _advance_member(case)
    assert _state(case) == (case.members, False, None)


@pytest.mark.parametrize("text_octets", [0, 4096])
@pytest.mark.growth_witness(
    "riverhog-imported-primary-terminal-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
)
def test_complete_terminal_import_work_depends_on_distinct_source_not_fanout(
    tmp_path, monkeypatch, record_property, text_octets
):
    material = _source_material(text_octets)
    samples = []
    shared_id = material.primary.journal.journal_id
    for fanout in (1, 4):
        with _staged_imports(tmp_path / str(fanout), monkeypatch, material, fanout=fanout) as case:
            _prepare_terminal(case)
            with _observe_terminal(case, monkeypatch) as observed:
                _terminal(case)
            assert _state(case) == (case.members, True, None)
            expected = {shared_id, *(primary.journal_id for primary in case.primaries)}
            assert set(observed.reads) == expected
            assert {row["journal_id"] for row in observed.validations} == expected
            assert {row["journal_id"] for row in observed.corpus_sources} == expected
            assert observed.resolutions == [
                (binding, RETAINED_HISTORY_EXTENT) for binding in case.histories
            ]
            assert len(observed.corpus_sources) == len(expected) == 5
            expected_frames = (
                material.full_anchor.through_sequence
                + 1
                + sum(
                    int(primary.binding.journal.model_dump(mode="json")["through"]["sequence"]) + 1
                    for primary in case.primaries
                )
            )
            assert sum(row["frames"] for row in observed.corpus_sources) == expected_frames
            shared_source = next(
                row for row in observed.corpus_sources if row["journal_id"] == shared_id
            )
            assert shared_source == {
                "journal_id": shared_id,
                "frames": 2,
                "octets": len(material.raw),
            }
            shared_reads = observed.reads[shared_id]
            assert len(shared_reads) == 3 and shared_reads[0]["octets"] == len(material.raw)
            assert shared_reads[0]["end"] is None
            assert sorted(row["octets"] for row in shared_reads[1:]) == sorted(
                (len(material.prefix), len(material.raw))
            )
            assert all(
                row["received"]
                and (row["exhausted"] if row["end"] is None else row["octets"] == row["end"])
                for rows in observed.reads.values()
                for row in rows
            )
            assert len(observed.imports) == fanout
            assert len(observed.raw_admissions) == len(observed.raw_current_checks) == 5
            assert {row["journal_id"] for row in observed.raw_admissions} == expected
            assert {row["journal_id"] for row in observed.raw_current_checks} == expected
            assert all(row["sql_statements"] == 2 for row in observed.raw_admissions)
            assert all(row["sql_statements"] == 1 for row in observed.raw_current_checks)
            assert all(row["sql_statements"] == 3 for row in observed.imports)
            assert observed.catalog_statements == (
                3 * len(expected) + 3 * fanout + observed.catalog_other_statements
            )
            assert observed.native_openings == 0
            assert len(observed.closures) == len(observed.corpora) == 1
            assert all(summary.storage_received for summary in observed.summaries)
            assert sum(row["native_advances"] for row in observed.validations) == observed.advances
            assert all(row["native_advances"] > 0 for row in observed.validations)
            assert all(
                owner._connection_closed and owner._directory_removed for owner in observed.closures
            )
            assert all(
                owner._closed
                and owner._connection_closed
                and not owner._owns_directory
                and not owner._directory.exists()
                for owner in observed.corpora
            )
            shared = [row for row in observed.validations if row["journal_id"] == shared_id]
            assert (shared[0]["owner"], shared[0]["anchor"]) == (
                "terminal",
                material.full_anchor.to_mapping(),
            )
            assert len(shared) == 3 and all(row["owner"] == "history" for row in shared[1:])
            assert {HistoryJournalAnchor.from_mapping(row["anchor"]) for row in shared[1:]} == {
                material.primary.journal,
                material.full_anchor,
            }
            samples.append(observed)
    first, repeated = samples
    assert first.advances == repeated.advances
    assert first.reads[shared_id] == repeated.reads[shared_id]
    assert next(row for row in first.corpus_sources if row["journal_id"] == shared_id) == next(
        row for row in repeated.corpus_sources if row["journal_id"] == shared_id
    )
    assert first.imports[0] == repeated.imports[0]
    assert repeated.imports == [first.imports[0]] * 4
    record_property(
        "growth_observations",
        {
            "members": 4,
            "fanout_small": 1,
            "fanout_large": 4,
            "documentary_text_octets": text_octets,
            "shared_prefix_octets": len(material.prefix),
            "shared_source_octets": len(material.raw),
            "shared_reads_small": len(first.reads[shared_id]),
            "shared_reads_large": len(repeated.reads[shared_id]),
            "shared_payload_octets_small": sum(row["octets"] for row in first.reads[shared_id]),
            "shared_payload_octets_large": sum(row["octets"] for row in repeated.reads[shared_id]),
            "native_advances_small": first.advances,
            "native_advances_large": repeated.advances,
            "native_validation_calls_small": len(first.validations),
            "native_validation_calls_large": len(repeated.validations),
            "corpus_source_frames_small": sum(row["frames"] for row in first.corpus_sources),
            "corpus_source_frames_large": sum(row["frames"] for row in repeated.corpus_sources),
            "corpus_source_octets_small": sum(row["octets"] for row in first.corpus_sources),
            "corpus_source_octets_large": sum(row["octets"] for row in repeated.corpus_sources),
            "raw_admissions_small": len(first.raw_admissions),
            "raw_admissions_large": len(repeated.raw_admissions),
            "raw_postchecks_small": len(first.raw_current_checks),
            "raw_postchecks_large": len(repeated.raw_current_checks),
            "raw_catalog_statements_small": sum(
                row["sql_statements"] for row in (*first.raw_admissions, *first.raw_current_checks)
            ),
            "raw_catalog_statements_large": sum(
                row["sql_statements"]
                for row in (*repeated.raw_admissions, *repeated.raw_current_checks)
            ),
            "terminal_catalog_statements_small": first.catalog_statements,
            "terminal_catalog_statements_large": repeated.catalog_statements,
            "other_catalog_statements_small": first.catalog_other_statements,
            "other_catalog_statements_large": repeated.catalog_other_statements,
            "import_sql_statements_small": sum(row["sql_statements"] for row in first.imports),
            "import_sql_statements_large": sum(row["sql_statements"] for row in repeated.imports),
            "import_native_metadata_queries_small": sum(
                row["native_metadata_queries"] for row in first.imports
            ),
            "import_native_metadata_queries_large": sum(
                row["native_metadata_queries"] for row in repeated.imports
            ),
            "import_native_metadata_octets_small": sum(
                row["metadata_octets"] for row in first.imports
            ),
            "import_native_metadata_octets_large": sum(
                row["metadata_octets"] for row in repeated.imports
            ),
        },
    )
    record_property(
        "imported_primary_profile",
        [
            {
                "validations": observed.validations,
                "imports": observed.imports,
                "corpus_sources": observed.corpus_sources,
                "raw_admissions": observed.raw_admissions,
                "raw_current_checks": observed.raw_current_checks,
                "elapsed_seconds": observed.elapsed_seconds,
                "thread_cpu_seconds": observed.thread_cpu_seconds,
            }
            for observed in samples
        ],
    )


def test_finalizer_checks_current_tail_after_the_selected_imported_primary(tmp_path, monkeypatch):
    material = _source_material()
    with _staged_imports(tmp_path, monkeypatch, material, fanout=1, members=1) as case:
        _advance_member(case)
        assert _state(case) == (1, False, None)
        identity = material.primary.journal.journal_id
        with session_scope(case.factory) as session:
            journal = session.get(Journal, (case.collection_id, identity))
            before = tuple(getattr(journal, column.key) for column in Journal.__table__.columns)
            chunk = session.get(Chunk, (case.collection_id, identity, 0))
            original = bytes(chunk.content)
            changed = original.replace(
                material.tail_text.encode(), b"R" + material.tail_text.encode()[1:], 1
            )
            assert changed != original and len(changed) == len(original)
            assert changed[: len(material.prefix)] == material.prefix
            chunk.content = changed
        with validate_journal(changed, catalog=material.catalog) as summary:
            assert summary.anchor != material.full_anchor.to_mapping()
            assert summary.anchor_at(0) == material.primary.journal.to_mapping()
        with session_scope(case.factory) as session:
            journal = session.get(Journal, (case.collection_id, identity))
            assert (
                tuple(getattr(journal, column.key) for column in Journal.__table__.columns)
                == before
            )
        with pytest.raises(
            ProvenanceValidationError, match="externally supplied prefix anchor mismatch"
        ):
            _owner(case).advance()
        assert _state(case) == (1, False, None)
        with session_scope(case.factory) as session:
            session.get(Chunk, (case.collection_id, identity, 0)).content = original
        _finish(case)


def _store(material):
    return MemberHistoryStore(
        lambda path: (material.objects[provenance_structure_object_id(path)],)
    )


def _historical_state(corpus, source):
    return corpus.selected_delivery_support(
        binding={"artifact_id": source.artifact_id, **source.primary.to_mapping()},
        artifact_id=source.artifact_id,
        byte_count=source.bytes,
        sha256=source.sha256,
        member_role=COLLECTION_MEMBER_ROLE,
        metadata_byte_budget=65536,
    ).state_reference


@pytest.mark.parametrize("extent", [BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT])
def test_imported_primary_resolver_keeps_exact_proof_state_and_requested_extent(extent):
    material = _source_material()
    calls = []
    with (
        validate_journal(material.raw, catalog=material.catalog) as complete,
        CanonicalCorpusValidator() as corpus,
    ):
        corpus.add(complete)

        def historical(source):
            calls.append(source)
            return _historical_state(corpus, source)

        def closure():
            return MemberHistoryClosure(
                _store(material),
                lambda _identity, end: (material.raw[:end],),
                member_role=COLLECTION_MEMBER_ROLE,
                catalog=material.catalog,
                imported_primary_state=historical,
            )

        imported = replace(material.imported, extent=extent)
        with closure() as current:
            current.resolve_import(imported)
            expected = (
                material.primary.journal if extent == BOUND_HISTORY_EXTENT else material.full_anchor
            )
            assert tuple(current.journal_anchors()) == (expected,)
        assert calls == [material.descriptor]
        with closure() as current, pytest.raises(ValueError):
            current.resolve_import(replace(imported, source_archive_root_sha256="0" * 64))
        assert calls == [material.descriptor]
        wrong = replace(imported, input_state={**imported.input_state, "assertion_id": new_id()})
        with closure() as current, pytest.raises(ProvenanceValidationError, match="source primary"):
            current.resolve_import(wrong)
        assert calls == [material.descriptor] * 2
        with validate_journal(material.prefix, catalog=material.catalog) as oracle:
            state, _ = selected_delivery_occurrence(
                oracle,
                binding={
                    "artifact_id": material.descriptor.artifact_id,
                    **material.primary.to_mapping(),
                },
                artifact_id=material.descriptor.artifact_id,
                byte_count=material.descriptor.bytes,
                sha256=material.descriptor.sha256,
                member_role=COLLECTION_MEMBER_ROLE,
            )
            assert external_reference(oracle, state["id"]) == imported.input_state


@pytest.mark.parametrize("pressure", ["entry-count", "source-octets"])
def test_imported_primary_point_facts_survive_real_summary_capacity(
    monkeypatch, record_property, pressure
):
    material = _source_material(4 * 1024 * 1024 if pressure == "source-octets" else 0)
    shared_id = material.primary.journal.journal_id
    sources = {shared_id: material.raw}
    reads, validations, points = defaultdict(list), [], []
    original = history_owner.validate_journal_chunks

    def read(identity, end):
        raw = sources[identity][:end]
        reads[identity].append(len(raw))
        yield raw

    def validated(chunks, **options):
        summary = original(chunks, **options)
        validations.append(summary.journal_id)
        return summary

    with (
        validate_journal(material.raw, catalog=material.catalog) as complete,
        CanonicalCorpusValidator() as corpus,
    ):
        corpus.add(complete)

        def historical(source):
            points.append(source)
            return _historical_state(corpus, source)

        with monkeypatch.context() as patch:
            patch.setattr(history_owner, "validate_journal_chunks", validated)
            with MemberHistoryClosure(
                _store(material),
                read,
                member_role=COLLECTION_MEMBER_ROLE,
                catalog=material.catalog,
                imported_primary_state=historical,
            ) as closure:
                closure.resolve_import(material.imported)
                assert sorted(reads[shared_id]) == sorted((len(material.prefix), len(material.raw)))
                if pressure == "entry-count":
                    original_keys = set(closure._summaries)
                    assert len(original_keys) == 2
                    for _ in range(32):
                        raw = create_journal(
                            {"agents": material.graph["agents"]},
                            recorded_by_agent_id=material.who,
                            catalog=material.catalog,
                        )
                        frame = parse_journal(raw)[0]
                        anchor = HistoryJournalAnchor.from_mapping(
                            {
                                "journal_id": frame.document["journal_id"],
                                "through": frame.reference,
                                "prefix_sha256": hashlib.sha256(raw).hexdigest(),
                                "prefix_bytes": str(len(raw)),
                            }
                        )
                        sources[anchor.journal_id] = raw
                        with closure.summary_at(anchor):
                            pass
                    assert len(closure._summaries) == 32
                    assert original_keys.isdisjoint(closure._summaries)
                else:
                    assert len(material.prefix) > 4 * 1024 * 1024
                    assert not closure._summaries
                before = len(validations)
                for _ in range(3):
                    closure.resolve_import(material.imported)
                assert len(validations) == before
                assert sorted(reads[shared_id]) == sorted((len(material.prefix), len(material.raw)))
                assert points == [material.descriptor] * 4
                assert tuple(closure.journal_anchors()) == (material.full_anchor,)
            assert closure._connection_closed and closure._directory_removed
    record_property(
        "growth_observations",
        {
            "imports": len(points),
            "shared_prefix_octets": len(material.prefix),
            "shared_provider_reads": len(reads[shared_id]),
            "shared_payload_octets": sum(reads[shared_id]),
            "shared_semantic_validations": validations.count(shared_id),
            "unrelated_summary_validations": len(validations) - validations.count(shared_id),
        },
    )


def test_terminal_import_rechecks_current_source_and_job_after_raw_validation(
    tmp_path, monkeypatch
):
    material = _source_material()
    original = import_owner.selected_upload_imported_primary_state
    with _staged_imports(tmp_path, monkeypatch, material, fanout=1, members=1) as case:
        _prepare_terminal(case)
        for fault, after in (("source", False), ("job", False), ("source", True), ("job", True)):
            calls = []

            def changed(
                session,
                collection_id,
                source,
                *,
                selected_fault=fault,
                seen=calls,
                after_support=after,
                **options,
            ):
                seen.append(source)

                def alter():
                    selected = Journal if selected_fault == "source" else Job
                    field = (
                        selected.capture_version
                        if selected_fault == "source"
                        else selected.worker_epoch
                    )
                    conditions = (
                        selected.collection_id == collection_id,
                        selected.journal_id == source.primary.journal.journal_id,
                    )
                    current = session.scalar(select(field).where(*conditions))
                    assert type(current) is int
                    session.execute(
                        update(selected).where(*conditions).values({field: current + 1})
                    )

                if not after_support:
                    alter()
                    return original(session, collection_id, source, **options)
                corpus = options["corpus"]
                actual_support = corpus.selected_delivery_support

                def support(**arguments):
                    result = actual_support(**arguments)
                    alter()
                    return result

                with monkeypatch.context() as patch:
                    patch.setattr(corpus, "selected_delivery_support", support)
                    return original(session, collection_id, source, **options)

            with monkeypatch.context() as patch:
                patch.setattr(import_owner, "selected_upload_imported_primary_state", changed)
                with pytest.raises(Conflict if after else LostJournalValidationWorkspace):
                    _terminal(case)
            assert calls == [material.descriptor]
            assert _state(case) == (1, False, None)
        _terminal(case)
