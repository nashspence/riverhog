from __future__ import annotations

import builtins
import io
import os
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.services import collection_uploads as owner
from riverhog_growth_contracts import growth_binding
from sqlalchemy import event, update
from sqlalchemy.exc import OperationalError

from tests.unit import test_upload_journal_service_coordination_growth as stamps
from tests.unit import test_upload_journal_service_unit_growth as units
from tests.unit.test_journal_capture import checkpoint
from tests.unit.test_journal_epoch_scan import finish as finish_epochs
from tests.unit.test_journal_validation_handoff import complete_capture, terminal
from tests.unit.test_staged_journal_range import JOURNAL
from tests.unit.test_upload_journal_source_growth import octets

ROOT = owner.SqlAlchemyCollectionUploadService._advance_provenance_journal_validation
CHILDREN = dict(owner._service_validation_advance_children())


class Recorder:
    def __init__(self, case, current):
        self.case, self.current = case, current
        self.units = units.UnitRecorder(case, current)
        self.active, self.depth = False, 0

    @contextmanager
    def child(self):
        self.depth += 1
        try:
            yield
        finally:
            self.depth -= 1

    def guard(self, actual):
        def guarded(*args, **kwargs):
            if self.active and self.depth == 0:
                self.own["unscoped_operations"] += 1
                raise AssertionError("service control performed work outside its declared child")
            return actual(*args, **kwargs)

        return guarded

    def stamped(self, factory, collection_id, journal_id):
        assert factory is self.case.factory
        with self.child():
            value, observed = stamps.catalog_measure(
                self.case,
                CHILDREN["stamp"],
                lambda: CHILDREN["stamp"](factory, collection_id, journal_id),
                (collection_id, journal_id),
                0,
                False,
            )
            self.children["stamp"].append(observed)
            if value is None:
                return None
            assert len(value) == 4
            self.operand_octets = max(self.operand_octets, octets(value))
            recorder = self

            class Stamp(tuple):
                def __ne__(self, other):
                    recorder.own["comparison_operations"] += 1
                    recorder.own["comparison_operand_fields"] += len(self) + (
                        len(other) if isinstance(other, tuple) else 0
                    )
                    recorder.own["comparison_operand_octets"] += octets(self) + octets(other)
                    return tuple.__ne__(self, other)

            return Stamp(value)

    def advanced(self, selected, collection_id, journal_id):
        assert selected is self.current
        with self.child():
            observed = self.units.measure(
                collection_id,
                journal_id,
                interrupt=self.interrupt,
                expected_error=self.expected_error,
            )
            self.children["unit"].append(observed)
            if self.units.last_error is not None:
                raise self.units.last_error
            return None

    def measure(self, collection_id=1, journal_id=JOURNAL, *, interrupt=False, expected_error=None):
        self.own = defaultdict(int)
        self.children = {tag: [] for tag in CHILDREN}
        self.operand_octets = 0
        self.interrupt, self.expected_error = interrupt, expected_error
        self.active, self.depth = False, 0
        error, result = None, None

        def sql(*_args):
            if self.active and self.depth == 0:
                self.own["unscoped_operations"] += 1
                raise AssertionError("service control performed unscoped SQL")

        event.listen(self.case.engine, "before_cursor_execute", sql)
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(owner, "_upload_journal_validation_stamp", self.stamped)
                patch.setattr(
                    type(self.current),
                    "_advance_provenance_journal_validation_unit",
                    lambda selected, collection_id, journal_id: self.advanced(
                        selected, collection_id, journal_id
                    ),
                )
                for module, name in (
                    (builtins, "open"),
                    (io, "open"),
                    (os, "open"),
                    (os, "read"),
                    (os, "write"),
                    (os, "stat"),
                    (os, "lstat"),
                    (os, "listdir"),
                    (os, "scandir"),
                    (os, "mkdir"),
                    (os, "unlink"),
                    (os, "rmdir"),
                    (os, "rename"),
                    (os, "replace"),
                    (sqlite3, "connect"),
                    (owner, "canonical_json_bytes"),
                ):
                    patch.setattr(module, name, self.guard(getattr(module, name)))
                patch.setattr(
                    owner,
                    "json",
                    SimpleNamespace(
                        loads=self.guard(owner.json.loads),
                        dumps=self.guard(owner.json.dumps),
                        JSONDecodeError=owner.json.JSONDecodeError,
                    ),
                )
                patch.setattr(
                    owner, "hashlib", SimpleNamespace(sha256=self.guard(owner.hashlib.sha256))
                )
                self.active = True
                try:
                    result = ROOT(self.current, collection_id, journal_id)
                except Exception as caught:
                    if expected_error is None or not isinstance(caught, expected_error):
                        raise
                    error = caught
                finally:
                    self.active = False
        finally:
            event.remove(self.case.engine, "before_cursor_execute", sql)
        self.last_error = error
        if expected_error is not None:
            assert isinstance(error, expected_error)
            assert error is self.units.last_error
        else:
            assert error is None and type(result) is bool
        dimensions = {"stamp_operand_octets": self.operand_octets}
        for tag, children in self.children.items():
            dimensions[tag + "_calls"] = len(children)
            self.own[tag + "_calls"] = self.own[tag + "_calls_limit"] = len(children)
            for item in growth_binding(CHILDREN[tag]).declaration.dimensions:
                dimensions[tag + "_" + item.name] = max(
                    (child["dimensions"][item.name] for child in children), default=0
                )
        self.own["stamp_sql_statements"] = sum(
            child["metrics"]["sql_statements"] for child in self.children[("stamp")]
        )
        self.own["stamp_returned_metadata_octets"] = sum(
            child["metrics"][("returned_metadata_octets")] for child in self.children[("stamp")]
        )
        self.own["unit_native_owner_sql_statements"] = sum(
            child["metrics"][("native_owner_sql_statements")] for child in self.children[("unit")]
        )
        return result, stamps.sample(ROOT, dimensions, self.own, self.children)


def advance_fixture(current, count):
    session = next(iter(current._journal_validation_owner._slots.values())).session
    for _ in range(10000):
        if (
            bool(session._input.pending)
            if count == 1
            else session._ledger.load().checkpoint.phase == ("terminal")
        ):
            return
        assert not session.advance().complete
    raise AssertionError("real Native fixture did not reach its selected boundary")


@pytest.mark.growth_witness(
    "riverhog-validation-stamped-service-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.services.collection_uploads.SqlAlchemyCollectionUploadService._advance_pro"
        "venance_journal_validation"
    ),
)
def test_actual_stamped_service_reports_durable_progress(tmp_path, record_property):
    samples = []
    for index, unrelated in enumerate((0, 1025)):
        for case, current in units.real_case(tmp_path / str(index), unrelated=unrelated):
            recorder = Recorder(case, current)
            for key in ("absent", "absent-" + "é" * 65537):
                result, observed = recorder.measure(journal_id=key)
                assert result is False
                assert observed["metrics"]["stamp_calls"] == 2
                assert observed["metrics"]["comparison_operations"] == 0
                samples.append(observed)
            prepared = 0
            for _ in range(20):
                result, observed = recorder.measure()
                assert result is True
                assert observed["metrics"]["comparison_operations"] == 1
                samples.append(observed)
                if terminal(case.factory)["state"] != "validating":
                    break
                count = recorder.units.native_calls
                if count in {1, 2} and prepared != count:
                    advance_fixture(current, count)
                    prepared = count
            else:
                raise AssertionError("small actual service fixture did not complete")
            assert terminal(case.factory)["state"] == "sealed"
            assert recorder.units.native_calls == 3
            result, observed = recorder.measure()
            assert result is False
            samples.append(observed)
            with case.factory() as session, session.begin():
                session.get(Journal, (1, JOURNAL)).capture_version = 2**200
            result, observed = recorder.measure()
            assert result is False
            assert observed["metrics"]["comparison_operand_octets"] >= 2 * len(str(2**200))
            samples.append(observed)
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-validation-stamped-service-rejections/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.services.collection_uploads.SqlAlchemyCollectionUploadService._advance_pro"
        "venance_journal_validation"
    ),
)
def test_actual_stamped_service_propagates_rejection_and_retries(tmp_path, record_property):
    samples = []
    for case, current in units.real_case(tmp_path / "rollback"):
        recorder = Recorder(case, current)
        before = checkpoint(case.factory)
        _, observed = recorder.measure(interrupt=True, expected_error=OperationalError)
        assert checkpoint(case.factory) == before
        assert observed["metrics"]["stamp_calls"] == 1
        assert observed["metrics"]["unit_calls"] == 1
        assert observed["metrics"]["comparison_operations"] == 0
        samples.append(observed)
        result, observed = recorder.measure()
        assert result is True and checkpoint(case.factory) != before
        samples.append(observed)
        held = next(iter(current._journal_validation_owner._slots.values())).lock
        assert held.acquire(blocking=False)
        before = checkpoint(case.factory)
        try:
            result, observed = recorder.measure()
        finally:
            held.release()
        assert result is False and checkpoint(case.factory) == before
        assert observed["metrics"]["stamp_calls"] == 2
        assert observed["metrics"]["unit_calls"] == 1
        samples.append(observed)
    for case, current in units.real_case(tmp_path / "malformed"):
        work = complete_capture(case.factory)
        finish_epochs(case.factory, work)
        with case.factory() as session, session.begin():
            session.execute(
                update(units.epochs.Scan).values(topology_cursor_json="x" * 65537)
            ).close()
        recorder = Recorder(case, current)
        before = checkpoint(case.factory)
        _, observed = recorder.measure(expected_error=RuntimeError)
        assert checkpoint(case.factory) == before
        assert observed["metrics"]["stamp_calls"] == 1
        assert observed["metrics"]["comparison_operations"] == 0
        assert observed["delegates"]["unit"][0]["metrics"]["core_json_decoded_octets"] >= 65537
        samples.append(observed)
    record_property("growth_samples", samples)


@pytest.mark.parametrize("first", ("collection_uploads", "upload_journal_validation_owner"))
def test_stamped_caller_has_exact_children_in_either_import_order(first):
    import subprocess
    import sys

    code = f"""
import importlib
from riverhog_growth_contracts import growth_binding
importlib.import_module("riverhog_core.services.{first}")
from riverhog_core.services import collection_uploads as owner
operation=owner.SqlAlchemyCollectionUploadService._advance_provenance_journal_validation
binding=growth_binding(operation)
assert binding.operation is operation
assert binding.declaration.id == "riverhog-server.validation-service-advance.growth/v1"
children=dict(binding.delegates)
assert children["stamp"] is owner._upload_journal_validation_stamp
assert children["unit"] is (
    owner.SqlAlchemyCollectionUploadService._advance_provenance_journal_validation_unit
)
"""
    subprocess.run([sys.executable, "-B", "-c", code], check=True, capture_output=True)
