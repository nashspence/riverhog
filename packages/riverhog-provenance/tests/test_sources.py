from __future__ import annotations

import io

import pytest
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    IncompleteSourceError,
    ObservationError,
    ObservationPolicy,
    ObservationRequest,
    SegmentSource,
    StreamSource,
    validate_graph,
)


@pytest.mark.parametrize("payload", [b"", b"\x00\xff", b"{not valid JSON", bytes(range(256)) * 257])
def test_arbitrary_primary_bytes_are_not_interpreted(payload, catalog):
    result = BoundedSourceObserver(catalog=catalog).observe(BytesSource(payload))
    assert result.observation["content"]["size_bytes"] == str(len(payload))
    assert "path" not in result.observation
    assert "contexts" not in result.graph_fragment()
    validate_graph(result.graph_fragment(), catalog=catalog)


def test_one_pass_source_requires_no_seek_fileno_name_or_host(catalog):
    class Reader:
        def __init__(self):
            self.parts = [b"ab", b"cd", b""]

        def read(self, size=-1):
            return self.parts.pop(0)

    source = StreamSource(Reader(), expected_length=4)
    result = BoundedSourceObserver(catalog=catalog).observe(source)
    assert result.observation["consistency"]["level"] == "one_pass"
    assert result.observation["address_status"] == "not_exposed"
    assert result.occurrence["kind"] == "stream_emission"
    with pytest.raises(ObservationError):
        BoundedSourceObserver(catalog=catalog).observe(source)


def test_caller_owned_reader_is_not_closed(catalog):
    reader = io.BytesIO(b"hello")
    BoundedSourceObserver(catalog=catalog).observe(StreamSource(reader))
    assert not reader.closed


def test_segment_does_not_consume_next_byte(catalog):
    reader = io.BytesIO(b"firstSECOND")
    source = SegmentSource(
        reader,
        length=5,
        boundary_policy_uri="urn:test:byte-offsets",
        start_boundary={"type": "integer", "value": "0"},
        end_boundary={"type": "integer", "value": "5"},
    )
    result = BoundedSourceObserver(catalog=catalog).observe(source)
    assert result.observation["content"]["size_bytes"] == "5"
    assert reader.read() == b"SECOND"


@pytest.mark.parametrize("declared", [1, 4])
def test_mismatching_declared_lengths_never_emit_states(declared, catalog):
    with pytest.raises(IncompleteSourceError):
        BoundedSourceObserver(catalog=catalog).observe(
            StreamSource(io.BytesIO(b"abc"), expected_length=declared)
        )


def test_unbounded_reader_hits_explicit_budget_without_emitting_truncated_state(catalog):
    class Endless:
        def read(self, size=-1):
            return b"x" * size

    with pytest.raises(IncompleteSourceError):
        BoundedSourceObserver(catalog=catalog).observe(
            StreamSource(Endless()),
            ObservationRequest(policy=ObservationPolicy(maximum_content_bytes=32)),
        )


def test_second_hash_is_not_silently_downgraded(catalog):
    with pytest.raises(ObservationError):
        BoundedSourceObserver(catalog=catalog).observe(
            StreamSource(io.BytesIO(b"x")),
            ObservationRequest(policy=ObservationPolicy(second_content_hash=True)),
        )
    result = BoundedSourceObserver(catalog=catalog).observe(
        BytesSource(b"x"),
        ObservationRequest(policy=ObservationPolicy(second_content_hash=True, include_sha512=True)),
    )
    assert len(result.observation["content"]["digests"]) == 2


def test_result_graph_is_deeply_isolated(observation):
    first = observation.graph_fragment()
    first["states"][0]["extent"]["kind"] = "made-up"
    assert observation.graph_fragment()["states"][0]["extent"]["kind"] == "whole_object"


@pytest.mark.parametrize(
    "field,value",
    [
        ("hash_chunk_bytes", 0),
        ("hash_chunk_bytes", True),
        ("maximum_content_bytes", -1),
        ("second_content_hash", 1),
    ],
)
def test_policy_admits_only_explicit_typed_limits(field, value):
    with pytest.raises((ValueError, TypeError)):
        ObservationPolicy(**{field: value})


def test_faulty_reader_contract_fails(catalog):
    class Reader:
        def read(self, size=-1):
            return "not bytes"

    with pytest.raises(ObservationError):
        BoundedSourceObserver(catalog=catalog).observe(StreamSource(Reader()))
