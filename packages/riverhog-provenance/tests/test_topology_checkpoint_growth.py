from __future__ import annotations

import hashlib
import sys
from contextlib import contextmanager
from types import CodeType, SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import topology_checkpoint as owner
from riverhog_provenance.topology_facts import TopologyArrayCursor, TopologyFrameCursor

ENCODE = owner.TopologyCaptureCheckpoint.to_json
DECODE = owner.TopologyCaptureCheckpoint.from_json.__func__
DIGEST = owner.validate_checkpoint_digest


@contextmanager
def observe(operation, monkeypatch):
    metrics = {bound.metric: 0 for bound in growth_binding(operation).declaration.bounds}
    digest_code = next(
        value
        for value in DIGEST.__code__.co_consts
        if isinstance(value, CodeType) and value.co_name == "<genexpr>"
    )

    def trace(frame, event, value):
        if frame.f_code is digest_code and event == "return" and type(value) is bool:
            metrics["digest_character_visits"] += 1
        return trace

    with monkeypatch.context() as patch:
        loads, encode = owner.json.loads, owner.canonical_json_bytes

        def decoded(raw):
            metrics["json_decodes"] += 1
            metrics["json_decoded_octets"] += len(raw.encode())
            return loads(raw)

        def encoded(value):
            raw = encode(value)
            metrics["json_encodes"] += 1
            metrics["json_encoded_octets"] += len(raw)
            return raw

        patch.setattr(owner, "json", SimpleNamespace(loads=decoded))
        patch.setattr(owner, "canonical_json_bytes", encoded)
        for name, calls, octets in (
            ("parse_scalar", "scalar_decodes", "scalar_decoded_octets"),
            ("format_scalar", "scalar_encodes", "scalar_encoded_octets"),
        ):
            actual = getattr(owner, name)

            def scalar(domain, value, _actual=actual, _calls=calls, _octets=octets):
                metrics[_calls] += 1
                if _calls == "scalar_decodes" and isinstance(value, str):
                    metrics[_octets] += len(value.encode())
                result = _actual(domain, value)
                if _calls == "scalar_encodes":
                    metrics[_octets] += len(result.encode())
                return result

            patch.setattr(owner, name, scalar)
        previous = sys.gettrace()
        sys.settrace(trace)
        try:
            yield metrics
        finally:
            sys.settrace(previous)


def sample(operation, dimensions, metrics):
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    result = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": metrics,
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [result])
    return result


def controls(count, *, active):
    cursor = (
        None
        if not active
        else TopologyFrameCursor(
            "b" * 64, TopologyArrayCursor(17, 99, 1, 3), TopologyArrayCursor(101, 300, 2, 17)
        )
    )
    return owner.TopologyCaptureCheckpoint(
        cursor, count, None if count == 0 else (count, "relations", count - 1, "a" * 64)
    )


def exercise(monkeypatch, record_property, operation):
    samples = []
    for retained in (1, 17, 1025):
        history = [controls(index, active=False) for index in range(1, retained + 1)]
        assert len({id(value) for value in history}) == retained
        for value in (
            controls(0, active=False),
            controls(17, active=True),
            controls((1 << 256) + 1, active=True),
            controls(1 << 15000, active=True),
        ):
            raw = value.to_json()
            for _ in range(2):
                with observe(operation, monkeypatch) as metrics:
                    result = (
                        operation(value)
                        if operation is ENCODE
                        else operation(owner.TopologyCaptureCheckpoint, raw)
                    )
                assert result == (raw if operation is ENCODE else value)
                samples.append(
                    sample(
                        operation,
                        {
                            "control_octets": len(raw.encode()),
                            "retained_headers": len(history),
                        },
                        metrics,
                    )
                )
        if operation is DECODE:
            for raw in (
                "",
                "x" * 65537,
                '{"active":null,"facts":"00","last":null}',
                controls(17, active=True).to_json() + " ",
                None,
            ):
                with observe(operation, monkeypatch) as metrics:
                    with pytest.raises(RuntimeError):
                        operation(owner.TopologyCaptureCheckpoint, raw)
                samples.append(
                    sample(
                        operation,
                        {
                            "control_octets": len(raw.encode()) if isinstance(raw, str) else 0,
                            "retained_headers": len(history),
                        },
                        metrics,
                    )
                )
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "native-topology-checkpoint-encode/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.topology_checkpoint.TopologyCaptureCheckpoint.to_json",
)
def test_actual_complete_checkpoint_encoding_width_and_independent_history(
    monkeypatch, record_property
):
    exercise(monkeypatch, record_property, ENCODE)


@pytest.mark.growth_witness(
    "native-topology-checkpoint-decode/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.topology_checkpoint.TopologyCaptureCheckpoint.from_json",
)
def test_actual_checkpoint_decoding_rejected_controls_and_full_scalar_domain(
    monkeypatch, record_property
):
    exercise(monkeypatch, record_property, DECODE)


@pytest.mark.growth_witness(
    "native-checkpoint-digest-shape/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.topology_checkpoint.validate_checkpoint_digest",
)
def test_actual_fixed_digest_iteration_including_late_rejection(monkeypatch, record_property):
    samples = []
    for value in (
        "a" * 64,
        "0" * 64,
        "A" + "a" * 63,
        "a" * 63 + "A",
        "a" * 63,
        "a" * 65537,
        None,
        b"a" * 64,
    ):
        with observe(DIGEST, monkeypatch) as metrics:
            try:
                result = DIGEST(value)
            except ValueError:
                result = None
            metrics["returned_octets"] = len(result.encode()) if result is not None else 0
        if value == "a" * 64:
            assert metrics["digest_character_visits"] == 64 and result is value
        if value == "a" * 63 + "A":
            assert metrics["digest_character_visits"] == 64 and result is None
        samples.append(
            sample(
                DIGEST,
                {
                    "supplied_octets": len(value.encode()) if isinstance(value, str) else 0,
                },
                metrics,
            )
        )
    record_property("growth_samples", samples)
