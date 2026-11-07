from __future__ import annotations

import hashlib
import threading
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_core.services import upload_journal_validation_owner as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from sqlalchemy import select

from tests.unit import test_upload_journal_owned_source_growth as fences
from tests.unit import test_upload_journal_validation_release_growth as release
from tests.unit.test_upload_journal_source_growth import octets

SWEEP = owner.UploadJournalValidationOwner.release_stale
SOURCE = fences.SOURCE
DROP = release.DROP
NATIVE = release.NATIVE_CLOSE
DIRECT = set(fences.BASE_METRICS) | {
    "pool_lock_acquisitions",
    "pool_lock_releases",
    "pool_map_lookups",
    "pool_map_removals",
    "pool_map_clears",
    "pool_slot_visits",
    "slot_lock_attempts",
    "blocking_slot_lock_attempts",
    "native_close_calls",
    "lease_release_callbacks",
    "lease_descriptor_closes",
    "catalog_calls",
    "path_probes",
    "path_opens",
    "path_syncs",
    "path_mutations",
    *(bound.metric for bound in growth_binding(NATIVE).declaration.bounds),
}


class Recorder(release.Recorder):
    def __init__(self):
        super().__init__()
        binding = growth_binding(SWEEP)
        projection = project_bindings((binding,))
        self.metadata[SWEEP] = (
            binding,
            projection,
            hashlib.sha256(
                canonical_json_bytes(projection["declarations"][binding.declaration.id])
            ).hexdigest(),
        )
        self.sources, self.drops, self.checked, self.dropped = [], [], [], []
        self.inputs = {}

    @contextmanager
    def kernel(self, engine, **_kwargs):
        with super().kernel(engine, catalog_reads=True):
            yield

    def invoke_drop(self, current, key, slot):
        before = self.values.copy()
        pending = len(current._slots)
        outer = self.delegates
        self.delegates = {"release": [], "retain": []}
        try:
            return DROP(current, key, slot)
        finally:
            children = self.delegates
            self.delegates = outer
            dimensions = {
                "configured_slots": current.concurrency,
                "pending_slots": pending,
                "release_calls": len(children["release"]),
                "retain_calls": len(children["retain"]),
            }
            metrics = {
                bound.metric: self.values[bound.metric] - before[bound.metric]
                for bound in growth_binding(DROP).declaration.bounds
            }
            metrics["native_close_classes"] = metrics["native_close_calls"]
            for tag, records in children.items():
                metrics[tag + "_calls"] = metrics[tag + "_calls_limit"] = len(records)
                for bound in growth_binding(NATIVE).declaration.bounds:
                    metrics[tag + "_" + bound.metric] = sum(
                        child["metrics"][bound.metric] for child in records
                    )
            node = self.sample(DROP, dimensions, metrics, children)
            validate_execution_samples(
                self.metadata[DROP][1], growth_binding(DROP).declaration.id, [node]
            )
            self.drops.append(node)
            self.dropped.append(key)

    def measure(self, case, current, selected, states, *, cold, busy):
        self.values = {bound.metric: 0 for bound in growth_binding(SWEEP).declaration.bounds}
        self.sources, self.drops, self.checked, self.dropped = [], [], [], []
        self.delegates = {}
        for key, slot, _storage in selected:
            context = SimpleNamespace(engine=case.engine, factory=case.factory, work=slot.work)
            worker_good = states.get(key) not in {"worker", "missing"}
            source_good = states.get(key) not in {"source", "binding"}
            slot.source.source_success = worker_good and source_good
            inputs, _rows = fences.input_dimensions(
                context, slot.source, slot.source.certificates, worker_good, source_good
            )
            with case.factory() as session:
                raw = session.scalar(
                    select(fences.certificates_owner.Scan.replay_binding_json).where(
                        fences.certificates_owner.Scan.collection_id == slot.work.collection_id,
                        fences.certificates_owner.Scan.journal_id == slot.work.journal_id,
                    )
                )
            if not source_good and states.get(key) == "source":
                raw = None
            fields = inputs[fences.CURRENT]
            fields["binding_present"], fields["binding_octets"] = int(raw is not None), octets(raw)
            inputs[SOURCE].update({"current_" + name: value for name, value in fields.items()})
            self.inputs[id(slot.source)] = inputs
        source_fields = growth_binding(SOURCE).declaration.dimensions
        dimensions = {
            "configured_slots": current.concurrency,
            "pending_slots": len(current._slots),
            "source_calls": 0,
            "drop_calls": 0,
        }
        for item in source_fields:
            dimensions["source_" + item.name] = max(
                (inputs[SOURCE][item.name] for inputs in self.inputs.values()), default=0
            )
        for item in growth_binding(DROP).declaration.dimensions:
            dimensions["drop_" + item.name] = (
                current.concurrency if item.name in {"configured_slots", "pending_slots"} else 1
            )
        self.envelopes = dimensions
        source_recorder = fences.Recorder(SOURCE, {})
        pool = release.Pool(self)
        pool.update(current._slots)
        current._slots = pool
        before = fences.snapshot(case.engine)
        keys = tuple(current._slots)
        caught = None
        held = []
        try:
            for index, (key, slot, _storage) in enumerate(selected):
                if busy and index % 2 == 0:
                    assert slot.lock.acquire(blocking=False)
                    held.append((key, slot))
            with source_recorder.observe(case.engine, cold=cold):
                observed_source = owner._OwnedSource.verify_source

                def source_call(selected_source):
                    source_recorder.inputs = self.inputs[id(selected_source)]
                    self.checked.append(
                        (
                            selected_source.job.collection_id,
                            selected_source.job.journal_id,
                            selected_source.source.source.attempt_id,
                        )
                    )
                    self.source_active = True
                    try:
                        return observed_source(selected_source)
                    finally:
                        self.source_active = False
                        self.sources.append(source_recorder.sample)

                with pytest.MonkeyPatch.context() as patch:
                    patch.setattr(owner._OwnedSource, "verify_source", source_call)
                    patch.setattr(
                        owner.UploadJournalValidationOwner,
                        "_drop",
                        lambda caller, key, slot: self.invoke_drop(caller, key, slot),
                    )
                    self.thread, self.active = threading.get_ident(), True
                    try:
                        try:
                            SWEEP(current)
                        except BaseException as error:
                            caught = error
                    finally:
                        self.active = False
            dimensions["source_calls"], dimensions["drop_calls"] = (
                len(self.sources),
                len(self.drops),
            )
            children = {"source": self.sources, "drop": self.drops}
            for tag, records in children.items():
                operation = SOURCE if tag == "source" else DROP
                for record in records:
                    assert all(
                        value <= dimensions[tag + "_" + name]
                        for name, value in record["dimensions"].items()
                    )
                    validate_execution_samples(
                        project_bindings((growth_binding(operation),)),
                        record["declaration"],
                        [record],
                    )
            metrics = {}
            for bound in growth_binding(SWEEP).declaration.bounds:
                metric = bound.metric
                if metric in DIRECT:
                    metrics[metric] = self.values[metric] + source_recorder.full.get(metric, 0)
                else:
                    metrics[metric] = sum(
                        child["metrics"].get(metric, 0)
                        for records in children.values()
                        for child in records
                    )
            metrics["connection_statements"] = (
                metrics["driver_statements"] - metrics["sql_statements"]
            )
            for tag, records in children.items():
                metrics[tag + "_calls"] = metrics[tag + "_calls_limit"] = len(records)
                operation = SOURCE if tag == "source" else DROP
                for bound in growth_binding(operation).declaration.bounds:
                    metrics[tag + "_" + bound.metric] = sum(
                        child["metrics"][bound.metric] for child in records
                    )
            sample = self.sample(SWEEP, dimensions, metrics, children)
            validate_execution_samples(
                self.metadata[SWEEP][1], growth_binding(SWEEP).declaration.id, [sample]
            )
            blocked = {key for key, _slot in held}
            if any(value == "binding" and key not in blocked for key, value in states.items()):
                assert isinstance(caught, RuntimeError)
            elif caught is not None:
                raise caught
            blocked = {key for key, _slot in held}
            expected = {
                key
                for key, value in states.items()
                if value in {"worker", "missing", "source", "closing"} and key not in blocked
            }
            assert set(self.dropped) == expected
            assert set(current._slots) == set(keys) - expected
            assert not (set(self.checked) & blocked)
            assert not any(key in self.checked for key, kind in states.items() if kind == "closing")
            assert fences.snapshot(case.engine) == before
            for key, slot, storage in selected:
                assert (slot.session is None) == (key in expected)
                if key in expected and not case.readers:
                    assert storage._closed
            assert not self.pool_held
            return sample
        finally:
            for _key, slot in held:
                slot.lock.release()


@contextmanager
def changes(case, selected, kind):
    saved = []
    states = {}
    with case.factory() as session, session.begin():
        for index, (key, slot, _storage) in enumerate(selected):
            if kind == "closing":
                slot.closing = True
                states[key] = kind
                continue
            if kind == "current" or kind in {"worker", "source"} and index % 2:
                continue
            if kind in {"missing", "binding"} and index:
                continue
            model = (
                jobs.Job
                if kind in {"worker", "missing"}
                else fences.certificates_owner.Scan
                if kind == "binding"
                else Journal
            )
            identity = (
                slot.job.workspace_id
                if model is jobs.Job
                else (slot.work.collection_id, slot.work.journal_id)
            )
            row = session.get(model, identity)
            fields = {column.key: getattr(row, column.key) for column in model.__table__.columns}
            saved.append((model, identity, fields))
            states[key] = kind
            if kind == "worker":
                row.worker_epoch += 1
            elif kind == "missing":
                session.delete(row)
            elif kind == "binding":
                row.replay_binding_json = "{"
            else:
                row.state = "failed"
    try:
        yield states
    finally:
        with case.factory() as session, session.begin():
            for model, identity, fields in saved:
                row = session.get(model, identity)
                if row is None:
                    session.add(model(**fields))
                else:
                    for name, value in fields.items():
                        setattr(row, name, value)


Journal = fences.certificates_owner.Journal


@contextmanager
def build_cases(root, *, large=True):
    with release.build_cases(root, large=large) as cases:
        yield cases


@pytest.fixture(scope="module")
def sweep_cases(tmp_path_factory):
    with build_cases(tmp_path_factory.mktemp("owner-sweep-inputs")) as cases:
        yield cases


@pytest.mark.growth_witness(
    "riverhog-validation-owner-stale-sweep-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner.release_stale",
)
def test_actual_stale_sweeps_preserve_heterogeneous_inputs_and_nonblocking_slots(
    sweep_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    for case in sweep_cases:
        for kind in ("current", "worker", "source", "missing", "binding", "closing"):
            for busy in (False, True):
                if busy and not case.works:
                    continue
                with release.live_owner(case, recorder_type=Recorder) as (
                    current,
                    recorder,
                    selected,
                    _borrowed,
                ):
                    with changes(case, selected, kind) as states:
                        samples.append(
                            recorder.measure(case, current, selected, states, cold=busy, busy=busy)
                        )
