from __future__ import annotations

import hashlib
import sqlite3
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    ArchiveProvenanceIdentity,
    CollectionArchiveManifest,
    CollectionArtifactSetIdentity,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryBuilder,
    MemberHistoryDocument,
    MemberHistoryImport,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    MemberHistoryStore,
    ProvenanceRootDocument,
    ProvenanceRootIdentity,
    SourceMemberHistoryBindingProof,
    member_history_object_path,
    provenance_structure_identity,
    source_binding_proof_object_path,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_primary_work as owner_module
from riverhog_core.canonical_discovery_archive_work import (
    ArchiveTaskKey,
    StaleArchiveTask,
    admit_archive_journal,
    apply_archive_descriptor,
    archive_metadata_progress,
    begin_archive_metadata,
    initialize_archive_work,
    prepare_archive_descriptor,
)
from riverhog_core.canonical_discovery_history_work import (
    ArchiveHistoryWork,
    HistorySetWork,
    apply_history_descriptor,
    apply_history_import,
    apply_history_record,
    history_set_progress,
    initialize_history_work,
    prepare_history_descriptor,
    prepare_history_import,
    prepare_history_record,
)
from riverhog_core.canonical_discovery_journal_work import StaleJournalTask
from riverhog_core.canonical_discovery_member_work import (
    ArchiveMemberRegistration,
    apply_archive_member,
    archive_member_progress,
    begin_archive_members,
    initialize_member_work,
    prepare_archive_member,
)
from riverhog_core.canonical_discovery_primary_work import (
    HistoryPrimaryWork,
    apply_history_primary,
    initialize_primary_work,
    prepare_history_primary,
    verify_history_import_primary,
)
from riverhog_core.canonical_discovery_snapshot_work import (
    HistorySnapshotWork,
    apply_history_snapshot,
    initialize_snapshot_work,
    prepare_history_snapshot,
)
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    CanonicalCorpusValidator,
    CorpusReplayBinding,
    JournalValidationCapacity,
    ProvenanceValidationError,
    RetainedCorpusValidator,
    append_correction,
    assertion,
    assertion_reference,
    create_journal,
    external_reference,
    reference,
    validate_journal,
)
from riverhog_provenance_contracts import ContractCatalog

from tests.support.canonical_discovery_work import RecordedConnection, write
from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission


class PrimaryCorpusSource:
    def __init__(self, source, summary, selections, directory, catalog):
        self.source = source
        self.original = HistoryJournalAnchor.from_mapping(summary.anchor)
        self.selections = frozenset(selections)

        def digest(anchors):
            return hashlib.sha256(
                canonical_json_bytes(
                    sorted(canonical_json_bytes(anchor.to_mapping()).decode() for anchor in anchors)
                )
            ).hexdigest()

        self.binding = CorpusReplayBinding(
            attempt_id="a" * 32,
            directory_id=directory.name,
            source_manifest_sha256=digest((self.original,)),
            source_count=1,
            selection_manifest_sha256=digest(self.selections),
            selection_count=len(self.selections),
            policy_sha256=hashlib.sha256(COLLECTION_MEMBER_ROLE.encode()).hexdigest(),
            catalog_sha256=catalog.checkpoint_sha256(),
        )

    def verify_current(self):
        try:
            self.source.check_open()
            return True
        except StaleJournalTask:
            return False

    def admit_source(self, anchor):
        return self.verify_current() and anchor == self.original

    def admit_selection(self, anchor):
        return self.verify_current() and anchor in self.selections


@contextmanager
def primary_corpus(path, source, summary, store, binding, catalog, *, retained):
    with ExitStack() as resources:
        if retained:
            directory = path.parent / (path.stem + "-native")
            directory.mkdir(mode=0o700)
            selections = tuple(
                root.journal for root in store.roots(binding, extent=RETAINED_HISTORY_EXTENT)
            )
            inputs = PrimaryCorpusSource(source, summary, selections, directory, catalog)
            corpus = resources.enter_context(
                RetainedCorpusValidator.open_retained(
                    directory, inputs, worker_epoch=1, create=True, catalog=catalog
                )
            )
        else:
            corpus = resources.enter_context(CanonicalCorpusValidator())
        while not corpus.advance_source(summary, frame_budget=1).complete:
            pass
        if retained:
            for anchor in selections:
                corpus.select_source(anchor)
            while not corpus.advance_semantics(row_budget=1).complete:
                pass
            while not corpus.advance_dependencies(row_budget=1).complete:
                pass
            for kind in ("derivation", "specialization"):
                while not corpus.advance_order(kind, row_budget=1).complete:
                    pass
        yield corpus


@contextmanager
def primary_work(
    path,
    *,
    fault=None,
    retained=False,
    inherited_extent=None,
    import_fault=None,
    worker_id="worker",
):
    catalog, payload = ContractCatalog(), b"primary"
    artifact_id = hashlib.sha256(payload).hexdigest()
    observed = BoundedSourceObserver(catalog=catalog).observe(BytesSource(payload))
    graph, who = observed.graph_fragment(), observed.observer_agent_id
    context = assertion("context", who, kind="delivery")
    association = assertion(
        "delivery_association",
        who,
        delivery_context_id=context["id"],
        slot={"kind": "text", "text": "other" if fault == "slot" else artifact_id},
        role=COLLECTION_MEMBER_ROLE,
        state=reference(observed.state_id, "state"),
        verification_observation_id=next(
            row["id"] for row in graph["descriptions"] if row["type"] == "observation"
        ),
    )
    graph["contexts"] = [*graph.get("contexts", ()), context]
    graph["delivery_associations"] = [association]
    selected_raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
    with validate_journal(selected_raw, catalog=catalog) as selected:
        primary = MemberHistoryPrimary(
            HistoryJournalAnchor.from_mapping(selected.anchor),
            "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"
            if fault == "association"
            else association["id"],
        )
        state_reference = external_reference(selected, observed.state_id)
        occurrence_reference = external_reference(selected, selected.states[0]["occurrence_id"])
        target = assertion_reference(selected, association["assertion_id"])
    raw = append_correction(
        selected_raw,
        [target],
        reason="withdrawn at later prefix",
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    with validate_journal(raw, catalog=catalog) as summary:
        with MemberHistoryBuilder(
            artifact_id=artifact_id,
            bytes=len(payload) + (fault == "size"),
            sha256="0" * 64 if fault == "digest" else artifact_id,
            primary=primary,
        ) as builder:
            builder.add_root(
                MemberHistoryRoot(HistoryJournalAnchor.from_mapping(summary.anchor), "retained")
            )
            binding, descriptor = builder.seal()
            structures = {
                provenance_structure_identity(value).relative_path: value
                for value in builder.objects()
            }
        source_descriptor, imported = descriptor, None
        if inherited_extent is not None:
            _, source_objects, _, _ = archive_reader_fixture(
                raw, summary.journal_id, histories=(descriptor,), history_objects=structures
            )
            source_root = ProvenanceRootDocument.from_json_bytes(
                source_objects["provenance/root.json.age"]
            )
            archive = CollectionArchiveManifest(
                archive_generation=source_root.archive_generation,
                artifact_set=CollectionArtifactSetIdentity(
                    1, descriptor.bytes, source_root.artifact_set_sha256
                ),
                ordered_volume_sha256="f" * 64,
                provenance=ArchiveProvenanceIdentity(
                    source_root.identity,
                    ProvenanceRootIdentity(
                        id="provenance-root",
                        kind="provenance-root",
                        path="provenance/root.json.age",
                        plaintext_bytes=len(source_root.to_json_bytes()),
                        sha256=source_root.identity,
                        stored_bytes=1234,
                        stored_sha256="0" * 64,
                    ),
                ),
            )
            proof = SourceMemberHistoryBindingProof(
                "9" * 64,
                7,
                archive.to_json_bytes(),
                source_root.to_json_bytes(),
                MemberHistoryBinding(
                    descriptor.artifact_id,
                    descriptor.bytes,
                    descriptor.sha256,
                    descriptor.identity,
                    len(descriptor.to_json_bytes()),
                ),
                0,
                (),
            )
            input_state = dict(state_reference)
            if import_fault == "state":
                input_state["object_id"] = "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"
            elif import_fault == "assertion":
                input_state["assertion_id"] = "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"
            elif import_fault == "entry":
                input_state["entry"] = dict(input_state["entry"], sequence="1")
            imported = MemberHistoryImport(
                proof.source_identity,
                proof.collection_id,
                hashlib.sha256(proof.archive_root).hexdigest(),
                source_root.artifact_set_sha256,
                descriptor.artifact_id,
                descriptor.identity,
                proof.identity,
                inherited_extent,
                input_state,
            )
            structures[source_binding_proof_object_path(proof.identity)] = proof.to_json_bytes()
            structures[member_history_object_path(descriptor.identity)] = descriptor.to_json_bytes()
            with MemberHistoryBuilder(
                artifact_id=descriptor.artifact_id,
                bytes=descriptor.bytes,
                sha256=descriptor.sha256,
                primary=descriptor.primary,
            ) as builder:
                builder.add_root(
                    MemberHistoryRoot(HistoryJournalAnchor.from_mapping(summary.anchor), "retained")
                )
                builder.add_import(imported)
                binding, descriptor = builder.seal()
                structures.update(
                    {
                        provenance_structure_identity(value).relative_path: value
                        for value in builder.objects()
                    }
                )
        reader, objects, _, _ = archive_reader_fixture(
            raw, summary.journal_id, histories=(descriptor,), history_objects=structures
        )
        admission = discovery_archive_admission(
            ProvenanceRootDocument.from_json_bytes(objects["provenance/root.json.age"]),
            total_bytes=descriptor.bytes,
        )
        db = sqlite3.connect(path, isolation_level=None)
        try:
            initialize_archive_work(db)
            initialize_member_work(db)
            initialize_history_work(db)
            initialize_snapshot_work(db)
            initialize_primary_work(db)
            key = ArchiveTaskKey("build", admission.identity, worker_id)
            with write(db):
                begin_archive_metadata(db, key, admission)
            while not archive_metadata_progress(db, key).complete:
                page = prepare_archive_descriptor(db, key, admission, reader)
                with write(db):
                    apply_archive_descriptor(db, key, admission, page)
            with write(db):
                begin_archive_members(db, key, admission)
            registration = ArchiveMemberRegistration(admission, reader)
            while not archive_member_progress(db, key).complete:
                page = prepare_archive_member(db, key, registration)
                with write(db):
                    apply_archive_member(db, key, registration, page)
            history_owner = ArchiveHistoryWork(admission, reader)
            page = prepare_history_descriptor(db, key, history_owner, artifact_id)
            with write(db):
                apply_history_descriptor(db, key, history_owner, page)
            for authority in (descriptor.roots, descriptor.imports):
                work = HistorySetWork(admission, reader, authority)
                while not history_set_progress(db, key, work).complete:
                    page = prepare_history_record(db, key, work)
                    with write(db):
                        apply_history_record(db, key, work, page)
            if imported is not None:
                imported_set = HistorySetWork(admission, reader, descriptor.imports)
                page = prepare_history_import(db, key, imported_set, imported.key)
                with write(db):
                    apply_history_import(db, key, imported_set, page)
                for authority in (source_descriptor.roots, source_descriptor.imports):
                    work = HistorySetWork(admission, reader, authority)
                    while not history_set_progress(db, key, work).complete:
                        page = prepare_history_record(db, key, work)
                        with write(db):
                            apply_history_record(db, key, work, page)
            roots = HistorySetWork(admission, reader, descriptor.roots)
            with admit_archive_journal(db, key, admission, summary) as source:
                store = MemberHistoryStore(lambda path: (objects[path],))
                with primary_corpus(
                    path, source, summary, store, binding, catalog, retained=retained
                ) as corpus:
                    snapshots = HistorySnapshotWork(roots, source)
                    for selected in store.roots(binding, extent=RETAINED_HISTORY_EXTENT):
                        page = prepare_history_snapshot(db, key, snapshots, selected.key)
                        with write(db):
                            apply_history_snapshot(db, key, snapshots, page)
                    owner = HistoryPrimaryWork(
                        history_owner, source, corpus, member_role=COLLECTION_MEMBER_ROLE
                    )
                    yield (
                        db,
                        key,
                        owner,
                        descriptor,
                        state_reference,
                        occurrence_reference,
                        type(summary.frames),
                    )
        finally:
            db.close()


@pytest.mark.parametrize("retained", [False, True])
def test_historical_primary_verifies_native_delivery_and_retains_only_original_support(
    tmp_path, monkeypatch, retained
):
    with primary_work(tmp_path / "work.sqlite", retained=retained) as values:
        db, key, owner, descriptor, state, occurrence, frame_type = values

        def no_frames(*_args):
            raise AssertionError("primary verification replayed a source frame")

        monkeypatch.setattr(frame_type, "__getitem__", no_frames)
        monkeypatch.setattr(frame_type, "__iter__", no_frames)
        page = prepare_history_primary(
            db, key, owner, descriptor.identity, metadata_byte_budget=65536
        )
        with write(db):
            apply_history_primary(db, key, owner, page)
            apply_history_primary(db, key, owner, page)
        assert db.execute(
            "SELECT source_key,anchor_json,state_reference_json,occurrence_reference_json "
            "FROM discovery_history_primary_bindings"
        ).fetchall() == [
            (
                owner.source.source_key,
                canonical_json_bytes(descriptor.primary.journal.to_mapping()).decode(),
                canonical_json_bytes(state).decode(),
                canonical_json_bytes(occurrence).decode(),
            )
        ]


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("fault", ["slot", "association", "size", "digest"])
def test_primary_uses_native_member_and_fixity_policy_before_any_registration(
    tmp_path, fault, retained
):
    with primary_work(tmp_path / "work.sqlite", fault=fault, retained=retained) as values:
        db, key, owner, descriptor, *_ = values
        with pytest.raises(ProvenanceValidationError):
            prepare_history_primary(db, key, owner, descriptor.identity, metadata_byte_budget=65536)
        assert db.execute("SELECT count(*) FROM discovery_history_primary_bindings").fetchone() == (
            0,
        )


def test_primary_assertion_capacity_does_not_register_partial_support(tmp_path):
    with primary_work(tmp_path / "work.sqlite") as values:
        db, key, owner, descriptor, *_ = values
        with pytest.raises(JournalValidationCapacity):
            prepare_history_primary(db, key, owner, descriptor.identity, metadata_byte_budget=1)
        assert db.execute("SELECT count(*) FROM discovery_history_primary_bindings").fetchone() == (
            0,
        )
        page = prepare_history_primary(
            db, key, owner, descriptor.identity, metadata_byte_budget=65536
        )
        with write(db):
            apply_history_primary(db, key, owner, page)


@pytest.mark.parametrize(
    "fault",
    ["descriptor", "roots", "imports", "link", "header", "source", "worker", "role", "page"],
)
def test_changed_primary_inputs_or_exact_preparation_cannot_register_support(tmp_path, fault):
    with primary_work(tmp_path / "work.sqlite") as values:
        db, key, owner, descriptor, *_ = values
        page = prepare_history_primary(
            db, key, owner, descriptor.identity, metadata_byte_budget=65536
        )
        if fault == "descriptor":
            db.execute(
                "UPDATE discovery_history_descriptors SET descriptor_json=descriptor_json||' '"
            )
        elif fault in {"roots", "imports"}:
            authority = getattr(descriptor, fault)
            db.execute(
                "UPDATE discovery_history_sets SET complete=0 WHERE records_sha256=?",
                (authority.records_sha256,),
            )
        elif fault == "link":
            db.execute("UPDATE discovery_history_snapshot_links SET inclusion='retained'")
        elif fault == "header":
            db.execute("UPDATE discovery_snapshot_headers SET anchor_json='{}'")
        elif fault == "source":
            owner.source.source_key = "0" * 64
        elif fault == "worker":
            key = replace(key, worker_id="replaced")
        elif fault == "role":
            owner.member_role = "urn:test:changed-role"
        elif fault == "page":
            page = replace(page, state_reference_json="{}")
        with pytest.raises(StaleArchiveTask):
            with write(db):
                apply_history_primary(db, key, owner, page)
        assert db.execute("SELECT count(*) FROM discovery_history_primary_bindings").fetchone() == (
            0,
        )


def test_closed_native_source_cannot_commit_a_prepared_primary(tmp_path):
    with primary_work(tmp_path / "work.sqlite") as values:
        db, key, owner, descriptor, *_ = values
        page = prepare_history_primary(
            db, key, owner, descriptor.identity, metadata_byte_budget=65536
        )
        owner.source.close()
        with pytest.raises(StaleJournalTask):
            with write(db):
                apply_history_primary(db, key, owner, page)
        assert db.execute("SELECT count(*) FROM discovery_history_primary_bindings").fetchone() == (
            0,
        )


def test_primary_registration_rolls_back_then_retries_exact_support(tmp_path):
    class Interrupted(BaseException):
        pass

    with primary_work(tmp_path / "work.sqlite") as values:
        db, key, owner, descriptor, *_ = values
        page = prepare_history_primary(
            db, key, owner, descriptor.identity, metadata_byte_budget=65536
        )
        with pytest.raises(Interrupted):
            with write(db):
                apply_history_primary(db, key, owner, page)
                raise Interrupted()
        assert db.execute("SELECT count(*) FROM discovery_history_primary_bindings").fetchone() == (
            0,
        )
        with write(db):
            apply_history_primary(db, key, owner, page)
        assert db.execute("SELECT count(*) FROM discovery_history_primary_bindings").fetchone() == (
            1,
        )


def test_primary_preparation_and_application_use_the_required_transaction_boundaries(tmp_path):
    with primary_work(tmp_path / "work.sqlite") as values:
        db, key, owner, descriptor, *_ = values
        with write(db):
            with pytest.raises(ValueError, match="no active transaction"):
                prepare_history_primary(
                    db, key, owner, descriptor.identity, metadata_byte_budget=65536
                )
        page = prepare_history_primary(
            db, key, owner, descriptor.identity, metadata_byte_budget=65536
        )
        with pytest.raises(ValueError, match="owner-managed transaction"):
            apply_history_primary(db, key, owner, page)


def growth_sample(operation, dimensions, metrics):
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": {
            bound.metric: metrics.get(bound.metric, 0) for bound in binding.declaration.bounds
        },
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return sample


@contextmanager
def observe_primary(db, owner, frame_type, monkeypatch):
    recorder = RecordedConnection(db)
    metrics = recorder.metrics
    actual_delivery = owner.corpus.selected_delivery_support
    actual_assertion = owner.corpus.historical_assertion
    actual_reference = owner.source.reference_anchor

    def delivery(**arguments):
        metrics["native_delivery_calls"] = metrics.get("native_delivery_calls", 0) + 1
        return actual_delivery(**arguments)

    def assertion(*arguments, **keywords):
        metrics["native_assertion_calls"] = metrics.get("native_assertion_calls", 0) + 1
        return actual_assertion(*arguments, **keywords)

    def reference(*arguments, **keywords):
        metrics["native_documentary_calls"] = metrics.get("native_documentary_calls", 0) + 1
        return actual_reference(*arguments, **keywords)

    def frame(*_arguments):
        metrics["source_frame_reads"] = metrics.get("source_frame_reads", 0) + 1
        raise AssertionError("primary work fetched an original source frame")

    with monkeypatch.context() as patch:
        patch.setattr(owner.corpus, "selected_delivery_support", delivery)
        patch.setattr(owner.corpus, "historical_assertion", assertion)
        patch.setattr(owner.source, "reference_anchor", reference)
        patch.setattr(frame_type, "__getitem__", frame)
        patch.setattr(frame_type, "__iter__", frame)
        yield recorder


@pytest.mark.growth_witness(
    "canonical-discovery-history-primary-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_primary_work.prepare_history_primary",
)
def test_actual_primary_preparation_uses_native_point_policy_and_fixed_core_reads(
    tmp_path, monkeypatch, record_property
):
    samples = []
    for retained in (False, True):
        with primary_work(tmp_path / f"measured-{retained}.sqlite", retained=retained) as values:
            db, key, owner, descriptor, _, _, frame_type = values
            for count in (0, 1):
                with observe_primary(db, owner, frame_type, monkeypatch) as observer:
                    page = prepare_history_primary(
                        observer, key, owner, descriptor.identity, metadata_byte_budget=65536
                    )
                samples.append(
                    growth_sample(
                        prepare_history_primary, {"registered_primaries": count}, observer.metrics
                    )
                )
                with write(db):
                    apply_history_primary(db, key, owner, page)
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "canonical-discovery-history-primary-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_primary_work.apply_history_primary",
)
def test_actual_primary_commit_writes_only_original_references_without_source_hashing(
    tmp_path, monkeypatch, record_property
):
    samples = []
    for retained in (False, True):
        with primary_work(tmp_path / f"measured-{retained}.sqlite", retained=retained) as values:
            db, key, owner, descriptor, _, _, frame_type = values
            page = prepare_history_primary(
                db, key, owner, descriptor.identity, metadata_byte_budget=65536
            )

            def no_source_hash(*_arguments):
                raise AssertionError("primary commit hashed source bytes")

            for count in (0, 1):
                with write(db):
                    with observe_primary(db, owner, frame_type, monkeypatch) as observer:
                        with monkeypatch.context() as patch:
                            patch.setattr(
                                owner_module, "hashlib", SimpleNamespace(sha256=no_source_hash)
                            )
                            apply_history_primary(observer, key, owner, page)
                samples.append(
                    growth_sample(
                        apply_history_primary, {"registered_primaries": count}, observer.metrics
                    )
                )
    record_property("growth_samples", samples)


def imported_primary(values):
    db, key, owner, descriptor, *_ = values
    imports = HistorySetWork(owner.history.admission, owner.history.reader, descriptor.imports)
    record, history = db.execute(
        "SELECT record_key,history_sha256 FROM discovery_history_import_links "
        "WHERE build_id=? AND admission_sha256=? AND import_set_sha256=?",
        (key.build_id, key.admission_sha256, descriptor.imports.records_sha256),
    ).fetchone()
    return db, key, owner, imports, record, history


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("extent", [BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT])
def test_import_state_matches_exact_native_verified_source_primary(tmp_path, retained, extent):
    with primary_work(
        tmp_path / "import.sqlite", retained=retained, inherited_extent=extent
    ) as values:
        db, key, owner, imports, record, history = imported_primary(values)
        with pytest.raises(StaleArchiveTask, match="Native-verified"):
            verify_history_import_primary(db, key, imports, record)
        page = prepare_history_primary(db, key, owner, history, metadata_byte_budget=65536)
        with write(db):
            apply_history_primary(db, key, owner, page)
        verify_history_import_primary(db, key, imports, record)
        with pytest.raises(ValueError, match="no active transaction"):
            with write(db):
                verify_history_import_primary(db, key, imports, record)


@pytest.mark.parametrize("fault", ["state", "assertion", "entry"])
def test_wrong_import_state_rejects_even_when_the_source_history_is_structurally_valid(
    tmp_path, fault
):
    with primary_work(
        tmp_path / "import.sqlite",
        retained=True,
        inherited_extent=BOUND_HISTORY_EXTENT,
        import_fault=fault,
    ) as values:
        db, key, owner, imports, record, history = imported_primary(values)
        page = prepare_history_primary(db, key, owner, history, metadata_byte_budget=65536)
        with write(db):
            apply_history_primary(db, key, owner, page)
        with pytest.raises(ProvenanceValidationError, match="source primary"):
            verify_history_import_primary(db, key, imports, record)


@pytest.mark.parametrize(
    "damage",
    [
        "coverage",
        "set",
        "link",
        "proof-worker",
        "primary-worker",
        "descriptor",
        "anchor",
        "source-history",
        "proof",
    ],
)
def test_import_primary_rejects_changed_original_proof_or_native_binding(tmp_path, damage):
    with primary_work(
        tmp_path / "import.sqlite", retained=True, inherited_extent=BOUND_HISTORY_EXTENT
    ) as values:
        db, key, owner, imports, record, history = imported_primary(values)
        page = prepare_history_primary(db, key, owner, history, metadata_byte_budget=65536)
        with write(db):
            apply_history_primary(db, key, owner, page)
        if damage == "coverage":
            db.execute("UPDATE discovery_history_import_coverage SET registered_imports='0'")
        elif damage == "set":
            db.execute(
                "UPDATE discovery_history_sets SET complete=0 WHERE records_sha256=?",
                (imports.authority.records_sha256,),
            )
        elif damage == "link":
            db.execute("DELETE FROM discovery_history_import_links")
        elif damage == "proof-worker":
            db.execute("UPDATE discovery_history_import_proofs SET worker_id='stale'")
        elif damage == "primary-worker":
            db.execute("UPDATE discovery_history_primary_bindings SET worker_id='stale'")
        elif damage == "descriptor":
            descriptor = MemberHistoryDocument.from_json_bytes(page.descriptor_json.encode())
            db.execute(
                "UPDATE discovery_history_descriptors SET descriptor_json=? WHERE history_sha256=?",
                (replace(descriptor, bytes=descriptor.bytes + 1).to_json_bytes().decode(), history),
            )
        elif damage == "anchor":
            db.execute("UPDATE discovery_history_primary_bindings SET anchor_json='{}'")
        elif damage == "source-history":
            db.execute("UPDATE discovery_history_import_proofs SET history_sha256=?", ("f" * 64,))
        else:
            db.execute("UPDATE discovery_history_import_links SET proof_sha256=?", ("f" * 64,))
        with pytest.raises(StaleArchiveTask):
            verify_history_import_primary(db, key, imports, record)
