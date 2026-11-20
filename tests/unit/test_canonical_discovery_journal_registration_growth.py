from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core import canonical_discovery_journal_registration as owner
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import replay_binding

from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_archive_framing_growth import observe as observe_framing
from tests.unit.test_canonical_discovery_journal_registration import DIRECTORY, register
from tests.unit.test_canonical_discovery_journal_source import source_case
from tests.unit.test_canonical_discovery_journal_source_growth import sample

OPERATIONS = {
    "find": owner.find_discovery_journal,
    "register": owner.register_discovery_journal,
    "confirm": owner.confirm_discovery_journal,
}


@contextmanager
def observe(case, work, monkeypatch):
    with observe_framing(case, work, monkeypatch) as recorder:
        for operation in OPERATIONS.values():
            for bound in growth_binding(operation).declaration.bounds:
                recorder.metrics.setdefault(bound.metric, 0)
        with monkeypatch.context() as patch:
            actual_progress = owner.framing.archive_framing_progress

            def progress(*args, **kwargs):
                before = dict(recorder.metrics)
                try:
                    return actual_progress(*args, **kwargs)
                finally:
                    recorder.metrics.update(before)
                    recorder.metrics["framing_progress_calls"] = (
                        before["framing_progress_calls"] + 1
                    )

            patch.setattr(owner.framing, "archive_framing_progress", progress)
            real_match = owner.re.fullmatch

            def match(pattern, value, *args, **kwargs):
                recorder.metrics["binding_regex_calls"] += 1
                recorder.metrics["binding_regex_octets"] += len(value.encode())
                return real_match(pattern, value, *args, **kwargs)

            patch.setattr(owner, "re", SimpleNamespace(fullmatch=match))
            patch.setattr(replay_binding, "re", SimpleNamespace(fullmatch=match))
            real_uuid = replay_binding.require_canonical_uuid_urn

            def uuid(value):
                recorder.metrics["binding_uuid_calls"] += 1
                recorder.metrics["binding_uuid_octets"] += len(value.encode())
                return real_uuid(value)

            patch.setattr(replay_binding, "require_canonical_uuid_urn", uuid)
            real_format = owner.format_scalar

            def scalar(domain, value):
                result = real_format(domain, value)
                recorder.metrics["scalar_encoded_octets"] += len(result.encode())
                return result

            patch.setattr(owner, "format_scalar", scalar)
            real_loads = replay_binding.json.loads

            def loads(value):
                recorder.metrics["json_decodes"] += 1
                recorder.metrics["json_decoded_octets"] += len(value.encode())
                return real_loads(value)

            patch.setattr(replay_binding, "json", SimpleNamespace(loads=loads))
            real_encode = replay_binding.canonical_json_bytes

            def encode(value):
                result = real_encode(value)
                recorder.metrics["json_encodes"] += 1
                recorder.metrics["json_encoded_octets"] += len(result)
                return result

            patch.setattr(replay_binding, "canonical_json_bytes", encode)
            yield recorder


def exercise(tmp_path, monkeypatch, record_property, mode):
    samples = []
    for frames, retained in ((1, 1), (17, 17), (1025, 1025), (1, 1025), (1025, 1)):
        with source_case(tmp_path / f"{frames}-{retained}", b"\x1e{}\n" * frames, retained=1) as (
            case,
            work,
            source,
        ):
            binding = replace(source.binding, directory_id=DIRECTORY)
            rows = []
            for number in range(retained):
                other = replace(
                    binding,
                    journal_id=f"urn:uuid:{number:08x}-2222-4222-8222-222222222222",
                    directory_id=f"{number:032x}",
                )
                rows.append(
                    (
                        case.metadata.key.build_id,
                        case.metadata.key.admission_sha256,
                        other.journal_id,
                        case.metadata.key.worker_id,
                        other.directory_id,
                        other.to_json(),
                    )
                )
            with case.metadata.transaction() as (db, _key):
                db.executemany(
                    "INSERT INTO discovery_journal_children VALUES(?,?,?,?,?,?,0)", rows
                ).close()
            selected = None if mode != "confirm" else register(case, work, binding)
            for retry in range(3):
                case.metadata._db.execute("PRAGMA shrink_memory").close()
                with case.metadata.read() as (db, _key):
                    count = db.execute(
                        "SELECT count(*) FROM discovery_journal_children"
                    ).fetchone()[0]
                dimensions = {
                    "retained_registrations": count,
                    "metadata_octets": octets(
                        (case.metadata.key, work.source.identity, binding, binding.to_json())
                    ),
                }
                context = case.metadata.read if mode == "find" else case.metadata.transaction
                with context() as (_db, key), observe(case, work, monkeypatch) as recorder:
                    changed_before = case.metadata._db.total_changes
                    if mode == "find":
                        result = owner.find_discovery_journal(recorder, key, work)
                    elif mode == "register":
                        result = owner.register_discovery_journal(recorder, key, work, binding)
                    else:
                        result = owner.confirm_discovery_journal(recorder, key, work, selected)
                    recorder.metrics["changed_rows"] = (
                        case.metadata._db.total_changes - changed_before
                    )
                    metrics = dict(recorder.metrics)
                samples.append(sample(OPERATIONS[mode], dimensions, metrics))
                if mode == "find" and retry == 0:
                    assert result is None
                    register(case, work, binding)
                elif mode == "find":
                    assert result.binding == binding and not result.confirmed
                elif mode == "register":
                    assert result.binding == binding and not result.confirmed
                else:
                    assert result
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-registration-find-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_registration.find_discovery_journal",
)
def test_actual_registration_lookup_keeps_unrelated_rows_independent(
    tmp_path, monkeypatch, record_property
):
    exercise(tmp_path, monkeypatch, record_property, "find")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-registration-register-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_registration.register_discovery_journal",
)
def test_actual_registration_creation_and_retries_are_indexed(
    tmp_path, monkeypatch, record_property
):
    exercise(tmp_path, monkeypatch, record_property, "register")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-registration-confirm-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_registration.confirm_discovery_journal",
)
def test_actual_open_confirmation_and_lost_reply_retries_are_indexed(
    tmp_path, monkeypatch, record_property
):
    exercise(tmp_path, monkeypatch, record_property, "confirm")
