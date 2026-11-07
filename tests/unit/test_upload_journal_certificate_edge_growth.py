from __future__ import annotations

import hashlib
from contextlib import ExitStack, contextmanager
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_certificates as owner
from riverhog_core.services import upload_journal_epochs as epochs
from riverhog_core.services import upload_journal_rank as rank
from riverhog_core.services import upload_journal_replay as replay
from riverhog_core.services import upload_journal_topology as topology
from riverhog_core.services import upload_journal_union as union
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import graph_certificates as native
from riverhog_provenance.errors import JournalValidationCapacity
from riverhog_provenance.graph_certificates import InvalidJournalGraphCertificate
from sqlalchemy import select, update

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_epoch_union import edge, raw_edges, uid
from tests.unit.test_journal_topology_capture import entry, raw_journal
from tests.unit.test_journal_validation_handoff import complete_capture, terminal
from tests.unit.test_upload_journal_certificate_lifecycle_growth import Recorder, snapshot
from tests.unit.test_upload_journal_source_growth import octets


def ready(factory):
    work = complete_capture(factory)
    for advance in (
        epochs.advance_upload_journal_epoch_scan,
        topology.advance_upload_journal_topology_capture,
        union.advance_upload_journal_epoch_union,
        rank.advance_upload_journal_epoch_rank,
    ):
        for _ in range(100000):
            done = advance(factory, work)
            state = terminal(factory)
            assert state["anchor"] is None and state["state"] == "validating"
            if done:
                break
        else:
            pytest.fail("finite preparation did not finish its admitted fixture")
    catalog = source_owner.admission_provenance_catalog()
    assert replay.prepare_upload_journal_replay(factory, work, catalog=catalog)
    return work, owner.UploadJournalGraphCertificates(factory, work, catalog=catalog)


@contextmanager
def build_cases(root, *, large=True):
    specifications = [
        (1, 1, 1, 0, "star", "derivation", False),
        (17, 1, 1, 0, "star", "specialization", False),
        (2, 1, 1, 0, "cycle", "derivation", False),
        (1, 1, 1, 0, "deferred", "derivation", False),
        (1, 1, 1, 0, "star", "derivation", True),
    ]
    if large:
        specifications.extend(
            (
                (129, 1, 1, 0, "chain", "derivation", False),
                (1025, 1, 1, 0, "star", "derivation", False),
                (1, 1025, 1, 0, "star", "derivation", False),
                (1, 1, 1025, 0, "star", "derivation", False),
                (1, 1, 1, 1024 * 1024, "star", "derivation", False),
            )
        )
    with ExitStack() as resources:
        cases = []
        for index, (edges, frames, journals, payload, shape, kind, wide) in enumerate(
            specifications
        ):
            values = (
                [edge(0, 0, 100), edge(1, 100, 0)]
                if shape == "cycle"
                else [edge(number, number, number + 1, kind=kind) for number in range(edges)]
                if shape == "chain"
                else [edge(number, kind=kind) for number in range(edges)]
            )
            if payload:
                values[0] = {**values[0], "padding": "x" * payload}
            raw = raw_edges(values) + raw_journal(
                *(entry(kind="checkpoint") for _ in range(frames - 1))
            )
            if shape == "deferred":
                raw += b"malformed tail"
            engine, factory = resources.enter_context(retained(root / str(index), raw))
            work, provider = ready(factory)
            if journals > 1:
                with factory() as session, session.begin():
                    selected = session.get(owner.Journal, (work.collection_id, work.journal_id))
                    fields = {
                        column.key: getattr(selected, column.key)
                        for column in owner.Journal.__table__.columns
                    }
                    session.add_all(
                        owner.Journal(**{**fields, "journal_id": uid(20000 + number)})
                        for number in range(journals - 1)
                    )
            if wide:
                with factory() as session, session.begin():
                    session.execute(update(owner.Scan).values(version=2**200)).close()
                provider = owner.UploadJournalGraphCertificates(
                    factory, work, catalog=source_owner.admission_provenance_catalog()
                )
            cases.append(
                SimpleNamespace(
                    engine=engine,
                    factory=factory,
                    work=work,
                    source=provider,
                    kind=kind,
                    target=uid(1 if shape == "chain" else 100),
                    frames=frames,
                    fallback=shape in {"cycle", "deferred"},
                )
            )
        yield cases


@pytest.fixture(scope="module")
def certificate_edge_cases(tmp_path_factory):
    with build_cases(tmp_path_factory.mktemp("certificate-edge-inputs")) as cases:
        yield cases


def options(case, *, frame=0):
    with case.factory() as session:
        digest = session.scalar(
            select(owner.Frame.epoch_json_sha256).where(owner.Frame.ordinal == frame)
        )
    return {
        "frame_ordinal": frame,
        "frame_sha256": digest,
        "kind": case.kind,
        "source": uid(0),
        "target": case.target,
        "byte_budget": 4096,
    }


def oracle(case, request):
    with case.factory() as session:
        frame = session.scalar(
            select(owner.Frame).where(owner.Frame.ordinal == request["frame_ordinal"])
        )
        epoch = None if frame is None else frame.epoch_ordinal

        def record(model, *extra):
            return session.scalar(
                select(model).where(
                    model.collection_id == case.work.collection_id,
                    model.journal_id == case.work.journal_id,
                    model.attempt_id == case.work.attempt_id,
                    model.epoch == epoch,
                    model.kind == request["kind"],
                    *extra,
                )
            )

        result, graph = record(owner.Result), record(owner.Graph)
        arc = record(
            owner.Arc,
            owner.Arc.source_id == request["source"],
            owner.Arc.target_id == request["target"],
        )
        left = record(owner.Ranked, owner.Ranked.vertex_id == request["source"])
        right = record(owner.Ranked, owner.Ranked.vertex_id == request["target"])

        def value(row, key):
            return None if row is None else getattr(row, key)

        row = (
            epoch,
            value(frame, "epoch_json_sha256"),
            value(result, "state"),
            value(result, "vertex_count"),
            value(result, "arc_count"),
            value(result, "settled_vertices"),
            value(result, "visited_arcs"),
            value(graph, "vertex_count"),
            value(graph, "arc_count"),
            value(graph, "state"),
            value(arc, "source_id"),
            value(left, "rank"),
            value(right, "rank"),
            value(left, "remaining_degree"),
            value(right, "remaining_degree"),
            value(left, "ready"),
            value(right, "ready"),
        )
    binding = case.source.binding.to_json().encode()
    proof = {
        "binding_sha256": hashlib.sha256(binding).hexdigest(),
        "frame_ordinal": request["frame_ordinal"],
        "frame_sha256": request["frame_sha256"],
        "epoch": row[0],
        "kind": request["kind"],
        "source": request["source"],
        "target": request["target"],
        "source_rank": row[11],
        "target_rank": row[12],
        "vertex_count": row[3],
        "arc_count": row[4],
    }
    encoded_proof = canonical_json_bytes({name: str(value) for name, value in proof.items()})
    return row, proof, len(binding), len(encoded_proof)


class EdgeRecorder(Recorder):
    def __init__(self, inputs):
        super().__init__()
        self.inputs, self.children = inputs, []
        self.metrics["validated_identity_octets"] = 0

    def primitive(self, function, before):
        values = {name: value - before.get(name, 0) for name, value in self.metrics.items()}
        values.update(
            connection_statements=values["driver_statements"] - values["sql_statements"],
            changed_rows=0,
            source_payload_octets=0,
        )
        binding = growth_binding(function)
        projection = project_bindings((binding,))
        return {
            "declaration": binding.declaration.id,
            "declaration_sha256": hashlib.sha256(
                canonical_json_bytes(projection["declarations"][binding.declaration.id])
            ).hexdigest(),
            "dimensions": {
                item.name: self.inputs[item.name] for item in binding.declaration.dimensions
            },
            "metrics": {bound.metric: values[bound.metric] for bound in binding.declaration.bounds},
            "delegates": {},
        }

    @contextmanager
    def observe(self, engine, *, cold):
        original_probe = owner.UploadJournalGraphCertificates._probe
        real_encode, real_hash = native.canonical_json_bytes, native.hashlib.sha256
        real_identity = owner.require_canonical_uuid_urn

        def encode(*args, **kwargs):
            value = real_encode(*args, **kwargs)
            self.metrics["json_encodes"] += 1
            self.metrics["json_encoded_octets"] += len(value)
            return value

        def identity(value):
            self.metrics["validated_identity_octets"] += octets(value)
            return real_identity(value)

        class Hash:
            def __init__(self, actual):
                self.actual = actual

            def update(inner, value):
                self.metrics["hashed_octets"] += len(value)
                inner.actual.update(value)

            def copy(inner):
                return Hash(inner.actual.copy())

            def __getattr__(inner, name):
                return getattr(inner.actual, name)

        def sha256(value=b"", *args, **kwargs):
            self.metrics["hashed_octets"] += len(value)
            return Hash(real_hash(value, *args, **kwargs))

        def probe(provider, **kwargs):
            before = self.metrics.copy()
            try:
                return original_probe(provider, **kwargs)
            finally:
                self.children.append(self.primitive(original_probe, before))

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(native, "canonical_json_bytes", encode)
            patch.setattr(native, "hashlib", SimpleNamespace(sha256=sha256))
            patch.setattr(native, "require_canonical_uuid_urn", identity)
            patch.setattr(owner, "require_canonical_uuid_urn", identity)
            patch.setattr(owner.UploadJournalGraphCertificates, "_probe", probe)
            with super().observe(engine, cold=cold):
                yield


def measure(case, function, request, *, cold, calls=1, error=None):
    before = snapshot(case.engine)
    if calls:
        expected_row, expected_proof, binding_octets, proof_octets = oracle(case, request)
    else:
        expected_row, expected_proof = None, None
        binding_octets, proof_octets = len(case.source.binding.to_json().encode()), 0
    inputs = {
        "retained_rows": sum(map(len, before.values())),
        "metadata_row_octets": max(octets(row) for rows in before.values() for row in rows),
        "probe_calls": calls,
        "request_octets": octets(request),
        "binding_octets": binding_octets,
        "proof_octets": proof_octets,
    }
    recorder = EdgeRecorder(inputs)
    binding = growth_binding(function)
    projection = project_bindings((binding,))
    result = None
    with recorder.observe(case.engine, cold=cold):
        if function is owner.UploadJournalGraphCertificates.edge:
            if error is None:
                result = case.source.edge(**request)
            else:
                with pytest.raises(error):
                    case.source.edge(**request)
        else:
            result = case.source._probe(
                **{name: request[name] for name in ("frame_ordinal", "kind", "source", "target")}
            )
    assert snapshot(case.engine) == before
    if function is owner.UploadJournalGraphCertificates.edge:
        values = recorder.metrics.copy()
        values.update(
            **{
                "probe_" + bound.metric: sum(
                    child["metrics"][bound.metric] for child in recorder.children
                )
                for bound in growth_binding(
                    owner.UploadJournalGraphCertificates._probe
                ).declaration.bounds
            }
        )
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": hashlib.sha256(
                canonical_json_bytes(projection["declarations"][binding.declaration.id])
            ).hexdigest(),
            "dimensions": inputs,
            "metrics": {bound.metric: values[bound.metric] for bound in binding.declaration.bounds},
            "delegates": {"probe": recorder.children},
        }
        if error is None:
            if case.fallback:
                assert result is None
            else:
                assert result is not None
                assert {name: getattr(result, name) for name in result.__slots__} == expected_proof
                assert result.source_rank < result.target_rank < result.vertex_count
                assert result.arc_count > 0 and result.encoded_octets() == proof_octets
    else:
        assert len(recorder.children) == 1 and result == expected_row
        sample = recorder.children[0]
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return sample


@contextmanager
def changed(case, fault):
    original = snapshot(case.engine)
    with case.factory() as session, session.begin():
        model = {
            "arc": owner.Arc,
            "rank": owner.Ranked,
            "order": owner.Ranked,
            "degree": owner.Ranked,
            "ready": owner.Ranked,
            "frame": owner.Frame,
            "manifest": owner.Scan,
            "source": owner.Journal,
            "counts": owner.Graph,
            "settled": owner.Result,
        }[fault]
        query = select(model)
        if model is owner.Ranked:
            query = query.where(model.vertex_id == case.target)
        row = session.scalars(query).first()
        assert row is not None
        fields = {column.key: getattr(row, column.key) for column in model.__table__.columns}
        key = tuple(getattr(row, column.key) for column in model.__table__.primary_key.columns)
        if fault in {"arc", "rank"}:
            session.delete(row)
        else:
            field, value = {
                "order": ("rank", 0),
                "degree": ("remaining_degree", 1),
                "ready": ("ready", True),
                "frame": ("epoch_json_sha256", fields.get("epoch_json_sha256", "")[:-1] + "f"),
                "manifest": ("version", fields.get("version", 0) + 1),
                "source": ("state", "failed"),
                "counts": ("arc_count", fields.get("arc_count", 0) + 1),
                "settled": ("settled_vertices", 0),
            }[fault]
            if fault == "frame" and value == fields[field]:
                value = value[:-1] + "e"
            setattr(row, field, value)
    try:
        yield
    finally:
        with case.factory() as session, session.begin():
            row = session.get(model, key)
            if row is None:
                session.add(model(**fields))
            else:
                for name, value in fields.items():
                    setattr(row, name, value)
        assert snapshot(case.engine) == original


def point_samples(cases, *, samples):
    operation = owner.UploadJournalGraphCertificates._probe
    for case in cases:
        for frame in sorted({0, case.frames - 1}):
            for cold in (False, True):
                samples.append(measure(case, operation, options(case, frame=frame), cold=cold))


def edge_samples(cases, *, samples):
    operation = owner.UploadJournalGraphCertificates.edge
    for case in cases:
        for frame in sorted({0, case.frames - 1}):
            for cold in (False, True):
                samples.append(measure(case, operation, options(case, frame=frame), cold=cold))
    case = cases[0]
    request = options(case)
    guards = (
        ({"byte_budget": 4095}, JournalValidationCapacity, 0),
        ({"byte_budget": True}, JournalValidationCapacity, 0),
        ({"frame_ordinal": -1}, InvalidJournalGraphCertificate, 0),
        ({"frame_ordinal": 2**200}, InvalidJournalGraphCertificate, 0),
        ({"kind": "x" * 65537}, InvalidJournalGraphCertificate, 0),
        ({"source": "x" * 65537}, InvalidJournalGraphCertificate, 0),
        ({"target": "x" * 65537}, InvalidJournalGraphCertificate, 0),
        (
            {
                "frame_sha256": request["frame_sha256"][:-1]
                + ("0" if request["frame_sha256"][-1] != "0" else "1")
            },
            InvalidJournalGraphCertificate,
            1,
        ),
        ({"target": uid(99999)}, InvalidJournalGraphCertificate, 1),
    )
    for change, error, calls in guards:
        for cold in (False, True):
            samples.append(
                measure(case, operation, {**request, **change}, cold=cold, calls=calls, error=error)
            )
    for fault in (
        "arc",
        "rank",
        "order",
        "degree",
        "ready",
        "frame",
        "manifest",
        "source",
        "counts",
        "settled",
    ):
        with changed(case, fault):
            for cold in (False, True):
                samples.append(
                    measure(
                        case, operation, request, cold=cold, error=InvalidJournalGraphCertificate
                    )
                )


@pytest.mark.growth_witness(
    "riverhog-certificate-point-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_certificates.UploadJournalGraphCertificates._probe",
)
def test_actual_certificate_lookup_includes_full_join_and_driver_work(
    certificate_edge_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    point_samples(certificate_edge_cases, samples=samples)


@pytest.mark.growth_witness(
    "riverhog-certificate-edge-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_certificates.UploadJournalGraphCertificates.edge",
)
def test_actual_certificate_edge_composes_probe_binding_hashes_and_proof(
    certificate_edge_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    edge_samples(certificate_edge_cases, samples=samples)
