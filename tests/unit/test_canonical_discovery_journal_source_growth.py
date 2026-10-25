from __future__ import annotations

import hashlib
from contextlib import contextmanager

import pytest
from riverhog_core import canonical_discovery_journal_source as source_owner
from riverhog_growth_contracts import (
    evaluate_bound,
    growth_binding,
    project_bindings,
    validate_execution_samples,
)
from riverhog_provenance import replay_binding

from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_archive_framing_growth import observe as observe_framing
from tests.unit.test_canonical_discovery_journal_source import source_case


@contextmanager
def observe(case, work, monkeypatch):
    metadata = case.metadata
    actual_read = metadata.read
    with observe_framing(case, work, monkeypatch) as recorder:
        for mode in ("__init__", "verify_current", "read_window", "read_exact"):
            for bound in growth_binding(
                getattr(source_owner.DiscoveryJournalSource, mode)
            ).declaration.bounds:
                recorder.metrics.setdefault(bound.metric, 0)
        with monkeypatch.context() as patch:
            # Child bodies run normally, retaining their own qualified bounds.
            # Only their real invocation counts belong to this assembly scope.
            @contextmanager
            def read():
                before, body = dict(recorder.metrics), None
                try:
                    with actual_read() as (_, key):
                        recorder.metrics.update(before)
                        recorder.metrics["metadata_read_calls"] = (
                            before.get("metadata_read_calls", 0) + 1
                        )
                        try:
                            yield recorder, key
                        finally:
                            body = dict(recorder.metrics)
                finally:
                    recorder.metrics.update(before if body is None else body)

            patch.setattr(metadata, "read", read)
            for owner, name, metric in (
                (source_owner.framing, "archive_framing_progress", "framing_progress_calls"),
                (source_owner.framing, "archive_framing_page", "framing_page_calls"),
                (work.source, "read_window", "original_window_calls"),
            ):
                actual = getattr(owner, name)

                def invoke(*args, _actual=actual, _metric=metric, **kwargs):
                    before = dict(recorder.metrics)
                    try:
                        return _actual(*args, **kwargs)
                    finally:
                        recorder.metrics.update(before)
                        recorder.metrics[_metric] = before.get(_metric, 0) + 1

                patch.setattr(owner, name, invoke)

            actual_binding = source_owner.JournalReplayBinding

            def binding(*args, **kwargs):
                recorder.metrics["binding_constructions"] = (
                    recorder.metrics.get("binding_constructions", 0) + 1
                )
                return actual_binding(*args, **kwargs)

            patch.setattr(source_owner, "JournalReplayBinding", binding)
            for owner, name, calls, width in (
                (replay_binding.re, "fullmatch", "binding_regex_calls", "binding_regex_octets"),
                (
                    replay_binding,
                    "require_canonical_uuid_urn",
                    "binding_uuid_calls",
                    "binding_uuid_octets",
                ),
            ):
                actual = getattr(owner, name)

                def validate(*args, _actual=actual, _calls=calls, _width=width, **kwargs):
                    recorder.metrics[_calls] = recorder.metrics.get(_calls, 0) + 1
                    value = args[1] if _calls == "binding_regex_calls" else args[0]
                    recorder.metrics[_width] = recorder.metrics.get(_width, 0) + len(value.encode())
                    return _actual(*args, **kwargs)

                patch.setattr(owner, name, validate)
            actual_format = source_owner.format_scalar

            def scalar(domain, value):
                result = actual_format(domain, value)
                recorder.metrics["scalar_encoded_octets"] = recorder.metrics.get(
                    "scalar_encoded_octets", 0
                ) + len(result.encode())
                return result

            patch.setattr(source_owner, "format_scalar", scalar)
            for name in ("_slice", "_join"):
                actual = getattr(source_owner, name)

                def copied(*args, _actual=actual):
                    result = _actual(*args)
                    recorder.metrics["copied_octets"] = recorder.metrics.get(
                        "copied_octets", 0
                    ) + len(result)
                    return result

                patch.setattr(source_owner, name, copied)
            yield recorder


def sample(operation, dimensions, metrics):
    from riverhog_canonical_json import canonical_json_bytes

    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    result = {
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
    try:
        validate_execution_samples(projection, binding.declaration.id, [result])
    except ValueError as error:
        raise AssertionError(
            [
                {
                    "metric": bound.metric,
                    "actual": result["metrics"][bound.metric],
                    "bound": evaluate_bound(binding, bound.metric, dimensions),
                }
                for bound in binding.declaration.bounds
                if result["metrics"][bound.metric]
                > evaluate_bound(binding, bound.metric, dimensions)
            ]
        ) from error
    return result


def exercise(tmp_path, monkeypatch, record_property, mode):
    extent = source_owner.framing.ARCHIVE_CAPTURE_PAGE_OCTETS
    operation = getattr(source_owner.DiscoveryJournalSource, mode)
    samples = []
    for pages, retained in ((1, 1), (2, 17), (17, 1025), (1, 1025), (17, 1)):
        raw = (b"\x1e" + b"x" * (extent - 2) + b"\n") * pages
        with source_case(tmp_path / f"{pages}-{retained}", raw, retained=retained) as (
            case,
            work,
            source,
        ):
            width = octets(
                (case.metadata.key, work.source.identity, source._progress, source.binding)
            )
            ranges = (
                (
                    (0, 0),
                    (0, 17),
                    (len(raw) - 19, 17),
                    (extent - 3, min(extent + 5, len(raw) - extent + 3)),
                )
                if mode in {"read_window", "read_exact"}
                else ((0, 0),)
            )
            for offset, requested in ranges:
                for cold in (True, False):
                    if cold:
                        source._page = None
                        case.metadata._db.execute("PRAGMA shrink_memory").close()
                    windows = (
                        (offset % extent + requested + extent - 1) // extent
                        if requested and mode == "read_exact"
                        else int(bool(requested) and mode == "read_window")
                    )
                    dims = {
                        "metadata_octets": width,
                        "requested_octets": requested,
                        "page_windows": windows,
                    }
                    with observe(case, work, monkeypatch) as recorder:
                        output = (
                            source_owner.DiscoveryJournalSource(
                                work, directory_id=case.metadata.job.workspace_id
                            )
                            if mode == "__init__"
                            else getattr(source, mode)(offset, requested)
                            if mode in {"read_window", "read_exact"}
                            else source.verify_current()
                        )
                    selected = output if mode == "__init__" else source
                    recorder.metrics["retained_page_octets"] = (
                        0 if selected._page is None else len(selected._page.content)
                    )
                    if mode in {"read_window", "read_exact"}:
                        assert (
                            output
                            == raw[
                                offset : offset
                                + (
                                    min(requested, extent - offset % extent)
                                    if mode == "read_window"
                                    else requested
                                )
                            ]
                        )
                        recorder.metrics["returned_source_octets"] = len(output)
                    samples.append(sample(operation, dims, recorder.metrics))
    if mode == "read_window":
        for retained in (1, 1025):
            raw = b"x" * 17
            with source_case(
                tmp_path / f"deferred-{retained}", raw, retained=retained, byte_budget=1
            ) as (case, work, source):
                width = octets(
                    (case.metadata.key, work.source.identity, source._progress, source.binding)
                )
                for cold in (True, False):
                    if cold:
                        work.source._page = None
                    offset = source._progress.cursor.byte_offset
                    dims = {
                        "metadata_octets": width,
                        "requested_octets": len(raw) - offset,
                        "page_windows": 0,
                    }
                    with observe(case, work, monkeypatch) as recorder:
                        output = source.read_window(offset, len(raw) - offset)
                    assert output == raw[offset:]
                    recorder.metrics["returned_source_octets"] = len(output)
                    recorder.metrics["retained_page_octets"] = 0
                    assert recorder.metrics["original_window_calls"] == 1
                    samples.append(sample(operation, dims, recorder.metrics))
    record_property("growth_samples", samples)
    record_property(
        "source_conditions",
        "Actual owned metadata and archive/framing directories; independent normalized page "
        "and unrelated-row counts, early/late/empty/cross-page ranges, cold/warm page caches, "
        "real binding validation, complete byte assembly and actual qualified child invocations.",
    )


@pytest.mark.growth_witness(
    "canonical-discovery-journal-source-construction/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_source.DiscoveryJournalSource.__init__",
)
def test_construct_source(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "__init__")


@pytest.mark.growth_witness(
    "canonical-discovery-journal-source-current/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_source.DiscoveryJournalSource.verify_current",
)
def test_current_source(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "verify_current")


@pytest.mark.growth_witness(
    "canonical-discovery-journal-source-window/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_source.DiscoveryJournalSource.read_window",
)
def test_window_source(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "read_window")


@pytest.mark.growth_witness(
    "canonical-discovery-journal-source-exact/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_source.DiscoveryJournalSource.read_exact",
)
def test_exact_source(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "read_exact")
