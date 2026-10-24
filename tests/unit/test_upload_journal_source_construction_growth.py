from __future__ import annotations

import builtins
import hashlib
import os
import sqlite3
import sys
from contextlib import contextmanager
from dataclasses import fields
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import upload_journal_replay as replay
from riverhog_core.services.upload_journal_source import UploadJournalValidationSource
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalValidationSession
from riverhog_provenance import replay_binding as codec
from riverhog_provenance_contracts import ContractCatalog
from riverhog_provenance_contracts import schemas as catalog_owner
from sqlalchemy import event

from tests.unit.test_upload_journal_source_growth import octets
from tests.unit.test_upload_journal_source_ports_growth import build_cases

CONSTRUCT = UploadJournalValidationSource.__init__
CALLS = {
    replay._binding.__code__: "binding_constructions",
    codec.JournalReplayBinding.__post_init__.__code__: "binding_validations",
    ContractCatalog.checkpoint_sha256.__code__: "cached_catalog_calls",
}


@pytest.fixture(scope="module")
def construction_cases(tmp_path_factory):
    catalog = replay.source_owner.admission_provenance_catalog()
    with build_cases(tmp_path_factory.mktemp("source-construction-inputs"), catalog) as cases:
        yield catalog, cases


@contextmanager
def observe(case, catalog, metrics):
    real_match, real_uuid = codec.re.fullmatch, codec.require_canonical_uuid_urn

    def forbidden(metric, *_args, **_kwargs):
        metrics[metric] += 1
        raise AssertionError("source construction crossed its execution boundary")

    def filesystem(*args, **kwargs):
        return forbidden("filesystem_calls", *args, **kwargs)

    def catalog_call(*args, **kwargs):
        return forbidden("catalog_calls", *args, **kwargs)

    def native(*args, **kwargs):
        return forbidden("native_session_calls", *args, **kwargs)

    def encode(*args, **kwargs):
        return forbidden("json_encodes", *args, **kwargs)

    def loads(*args, **kwargs):
        return forbidden("json_decodes", *args, **kwargs)

    def digest(*args, **kwargs):
        return forbidden("hashed_octets", *args, **kwargs)

    def fullmatch(pattern, value, *args, **kwargs):
        metrics["regex_calls"] += 1
        metrics["regex_input_octets"] += octets(value)
        return real_match(pattern, value, *args, **kwargs)

    def uuid(value):
        metrics["uuid_validations"] += 1
        metrics["uuid_input_octets"] += octets(value)
        return real_uuid(value)

    class CatalogState(dict):
        def __iter__(self):
            return forbidden("catalog_schema_visits")

        items = keys = values = __iter__

    previous_profile = sys.getprofile()

    def profile(frame, event, arg):
        if previous_profile is not None:
            previous_profile(frame, event, arg)
        if event == "call" and frame.f_code in CALLS:
            metrics[CALLS[frame.f_code]] += 1
        if event == "return" and frame.f_code is CONSTRUCT.__code__:
            binding = frame.f_locals["self"]._binding
            metrics["binding_fields"] = len(fields(binding))
            metrics["scalar_text_octets"] = octets(
                (binding.source_bytes, binding.source_version, binding.frame_count)
            )

    event.listen(case.engine, "before_cursor_execute", catalog_call)
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(builtins, "open", filesystem)
            for name in ("open", "stat", "lstat", "scandir", "listdir", "mkdir", "unlink"):
                patch.setattr(os, name, filesystem)
            patch.setattr(sqlite3, "connect", catalog_call)
            for name in ("open_retained", "progress", "advance", "result", "close"):
                patch.setattr(JournalValidationSession, name, native)
            patch.setattr(codec, "re", SimpleNamespace(fullmatch=fullmatch))
            patch.setattr(codec, "require_canonical_uuid_urn", uuid)
            patch.setattr(codec, "canonical_json_bytes", encode)
            patch.setattr(codec, "json", SimpleNamespace(loads=loads))
            patch.setattr(catalog_owner, "canonical_json_bytes", encode)
            patch.setattr(catalog_owner, "hashlib", SimpleNamespace(sha256=digest))
            patch.setattr(catalog, "_bindings", CatalogState(catalog._bindings))
            patch.setattr(catalog, "_validators", CatalogState(catalog._validators))
            sys.setprofile(profile)
            try:
                yield
            finally:
                sys.setprofile(previous_profile)
    finally:
        event.remove(case.engine, "before_cursor_execute", catalog_call)


def measure(catalog, case):
    binding = growth_binding(CONSTRUCT)
    projection = project_bindings((binding,))
    expected = replay._binding(case.work, catalog)
    metrics = {bound.metric: 0 for bound in binding.declaration.bounds}
    dimensions = {
        "source_control_octets": sum(
            octets(getattr(expected, field.name)) for field in fields(expected)
        )
    }
    with observe(case, catalog, metrics):
        selected = UploadJournalValidationSource(case.factory, case.work, catalog=catalog)
    assert selected.factory is case.factory and selected.source is case.work
    assert selected.binding == expected and selected._page is None
    assert case.engine.pool.checkedout() == 0
    sample = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": metrics,
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [sample])
    return sample


@pytest.mark.growth_witness(
    "riverhog-retained-source-construction-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_source.UploadJournalValidationSource.__init__",
)
def test_actual_source_construction_uses_exact_scalars_and_cached_catalog_identity(
    construction_cases, record_property
):
    catalog, cases = construction_cases
    samples = []
    record_property("growth_samples", samples)
    for case in cases:
        samples.append(measure(catalog, case))
        samples.append(measure(catalog, case))
