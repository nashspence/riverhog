from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping

import pytest
import riverhog_growth_contracts as growth
from riverhog_canonical_json import canonical_json_bytes

from scripts.validation_numeric import numeric_policy_identity, numeric_validation_receipt
from scripts.validation_witnesses import _validate_growth_execution, validate_growth_execution
from tests.support.growth_batch import SampleBatch

# Explicit resources for finite consumer fixtures, not an operational default.
POLICY = growth.NumericWorkPolicy(10_000_000, 256)


def dimension(name):
    return growth.Expression("dimension", name=name)


def count(value):
    return growth.Expression("constant", value=value, unit=growth.COUNT)


def declaration(name, dimensions, expression, *, relation="upper"):
    return growth.Declaration(
        id=f"fixture.consumer_{name}.growth/v1",
        authority="fixture",
        subjects=(f"fixture.consumer_{name}",),
        scope="finite-actual-child-consumer-fixture",
        starts_at="receive original observations",
        ends_at="validate complete original closure",
        dimensions=tuple(
            growth.Dimension(key, growth.COUNT, "Actual source-owned fixture input.")
            for key in dimensions
        ),
        bounds=(growth.Bound("work", growth.COUNT, relation, expression, "Actual fixture work."),),
        conditions=("Complete finite original observations.",),
        invariants=("Every original child remains independently checked.",),
        limitations=("No product execution or throughput claim.",),
    )


def attach(value, delegates=()):
    @growth.declare_growth(value, delegates=delegates)
    def operation():
        return None

    return growth.growth_binding(operation)


def sample(binding, dimensions, work, children=None):
    row = growth.project_binding(binding, numeric_policy=POLICY)
    return {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(canonical_json_bytes(row)).hexdigest(),
        "dimensions": dimensions,
        "metrics": {"work": work},
        "delegates": {} if children is None else children,
    }


def fixture(*, op="delegate_envelope", values=(4, 9)):
    leaf = attach(declaration("leaf", ("items",), dimension("items"), relation="equal"))
    parent = attach(
        declaration(
            "parent",
            ("calls", "ceiling"),
            growth.Expression(
                op,
                name="leaf",
                metric="work",
                terms=(dimension("calls"),),
                arguments=(("items", dimension("ceiling")),),
            ),
        ),
        (("leaf", leaf.operation),),
    )
    children = [sample(leaf, {"items": value}, value) for value in values]
    original = sample(
        parent,
        {"calls": len(values), "ceiling": 10},
        sum(values),
        {"leaf": children},
    )
    projection = growth.project_bindings((parent,), numeric_policy=POLICY)
    return parent, leaf, projection, original


def validate_roots(parent, projection, original, *, policy=POLICY):
    return growth.validate_execution_roots(
        canonical_json_bytes(projection),
        [(parent.declaration.id, [original])],
        numeric_policy=policy,
    )


def evidence(parent, projection, original):
    source = {"source_sha": "a" * 40, "source_clean": True}
    row = projection["declarations"][parent.declaration.id]
    record = {
        "format": "riverhog-growth-execution/v1",
        "source": source,
        "source_end": source,
        "run_id": "literal-envelope-consumer",
        "session_id": "literal-envelope-consumer",
        "node_id": "literal-envelope-consumer",
        "owner_sha256": "b" * 64,
        "declaration": {
            "id": parent.declaration.id,
            "sha256": hashlib.sha256(canonical_json_bytes(row)).hexdigest(),
            "projection_sha256": hashlib.sha256(canonical_json_bytes(projection)).hexdigest(),
        },
        "outcomes": {
            phase: {"seconds": 0.0, "outcome": "passed", "expected_failure": False}
            for phase in ("setup", "call", "teardown")
        },
        "samples": [original],
        "numeric_policy": numeric_policy_identity(POLICY),
        "numeric_validation": None,
    }
    usage = _validate_growth_execution(projection, record, source=source, numeric_policy=POLICY)
    record["numeric_validation"] = numeric_validation_receipt(POLICY, source, usage)
    return source, record, usage


def test_new_operator_roundtrips_distinctly_through_all_shared_readers():
    parent, leaf, projection, original = fixture()
    encoded = canonical_json_bytes(projection)
    decoded = json.loads(encoded)
    expression = decoded["declarations"][parent.declaration.id]["bounds"]["work"]["expression"]
    assert expression["op"] == "delegate_envelope"
    assert (
        growth.evaluate_bound(parent, "work", original["dimensions"], numeric_policy=POLICY) == 20
    )
    growth.validate_projection(decoded, numeric_policy=POLICY)
    growth.validate_execution_samples(
        decoded, parent.declaration.id, [original], numeric_policy=POLICY
    )
    usage = growth.validate_execution_roots(
        encoded,
        [(parent.declaration.id, [original]), (leaf.declaration.id, original["delegates"]["leaf"])],
        numeric_policy=POLICY,
    )
    assert usage.composition_nodes > 0 and usage.metric_calls > 0
    assert usage.consumed == usage.payload_work + usage.meter_work
    assert usage.maximum_operand_bits <= POLICY.max_integer_bits


def test_existing_exact_operator_still_rejects_heterogeneous_arguments():
    parent, _, projection, original = fixture(op="delegate")
    with pytest.raises(ValueError, match="actual delegate calls contradict"):
        validate_roots(parent, projection, original)
    parent, _, projection, original = fixture(op="delegate", values=(10, 10))
    validate_roots(parent, projection, original)


@pytest.mark.parametrize("defect", ["kind", "missing_arguments", "extra_field"])
def test_literal_operator_rejects_stale_or_contradictory_representation(defect):
    parent, _, projection, original = fixture()
    expression = projection["declarations"][parent.declaration.id]["bounds"]["work"]["expression"]
    if defect == "kind":
        expression["op"] = "delegate"
        with pytest.raises(ValueError, match="stale or mismatched declaration digest"):
            validate_roots(parent, projection, original)
    else:
        if defect == "missing_arguments":
            del expression["arguments"]
        else:
            expression["observed_maxima"] = {}
        with pytest.raises(ValueError, match="missing, unknown or contradictory"):
            validate_roots(parent, projection, original)


@pytest.mark.parametrize("reader", ["source", "projection", "roots"])
def test_prior_allowed_metric_proof_cannot_authorize_envelope_in_exact_count(reader):
    leaf = attach(declaration("a_leaf", ("items",), dimension("items"), relation="equal"))
    middle = attach(
        declaration(
            "b_middle",
            ("items",),
            growth.Expression(
                "delegate_envelope",
                name="leaf",
                metric="work",
                terms=(count(1),),
                arguments=(("items", dimension("items")),),
            ),
        ),
        (("leaf", leaf.operation),),
    )
    # The middle metric is sound in an ordinary upper-bound context. Its
    # cached proof cannot be reused when its result is an exact call count.
    indirect_count = growth.Expression(
        "delegate",
        name="middle",
        metric="work",
        terms=(count(1),),
        arguments=(("items", dimension("items")),),
    )
    invalid = declaration(
        "c_parent",
        ("items",),
        growth.Expression(
            "delegate",
            name="leaf",
            metric="work",
            terms=(indirect_count,),
            arguments=(("items", dimension("items")),),
        ),
    )
    delegates = (("leaf", leaf.operation), ("middle", middle.operation))
    if reader == "source":
        with pytest.raises(ValueError, match="envelope"):
            attach(invalid, delegates)
        return
    projection = growth.project_bindings((middle,), numeric_policy=POLICY)
    row = invalid.project()
    row["delegates"] = {
        name: {"declaration": growth.growth_binding(operation).declaration.id}
        for name, operation in delegates
    }
    projection["declarations"][invalid.id] = row
    with pytest.raises(ValueError, match="envelope"):
        if reader == "projection":
            growth.validate_projection(projection, numeric_policy=POLICY)
        else:
            growth.validate_execution_roots(
                canonical_json_bytes(projection),
                [(leaf.declaration.id, [sample(leaf, {"items": 1}, 1)])],
                numeric_policy=POLICY,
            )


def test_envelope_comparison_has_its_own_finite_meter_refusal(monkeypatch):
    parent, _, projection, original = fixture()
    comparison = growth._NumericWork.comparison
    prefixes = []

    def observe(work, left, right, operation):
        if operation == "composition_envelope_argument":
            prefixes.append(work.consumed)
        return comparison(work, left, right, operation)

    monkeypatch.setattr(growth._NumericWork, "comparison", observe)
    complete = validate_roots(parent, projection, original)
    assert len(prefixes) >= 2 and complete.consumed > prefixes[0]
    allowance = growth.NumericWorkPolicy(prefixes[0], POLICY.max_integer_bits)
    with pytest.raises(growth.NumericWorkRefused) as caught:
        validate_roots(parent, projection, original, policy=allowance)
    error = caught.value
    assert error.reason == "work_allowance_exhausted"
    assert error.operation.startswith("composition_envelope_argument")
    assert error.limit == allowance.max_work and error.consumed <= error.limit
    assert error.requested > 0


class CallbackInteger(int):
    def __lt__(self, _other):
        raise AssertionError("numeric guard invoked an original integer-subclass comparison")

    def __gt__(self, _other):
        raise AssertionError("numeric guard invoked an original integer-subclass comparison")

    def bit_length(self):
        raise AssertionError("numeric guard invoked an original integer-subclass method")


@pytest.mark.parametrize("replacement", [True, -1, CallbackInteger(9), 1 << 300])
def test_later_original_child_is_rechecked_after_its_recursive_validation(monkeypatch, replacement):
    parent, _, projection, original = fixture()
    comparison = growth._NumericWork.comparison
    changed = []

    def change(work, left, right, operation):
        if operation == "composition_envelope_argument" and not changed:
            original["delegates"]["leaf"][1]["dimensions"]["items"] = replacement
            changed.append(True)
        return comparison(work, left, right, operation)

    monkeypatch.setattr(growth._NumericWork, "comparison", change)
    with pytest.raises(ValueError) as caught:
        validate_roots(parent, projection, original)
    assert changed
    if replacement == 1 << 300:
        assert isinstance(caught.value, growth.NumericWorkRefused)
        assert caught.value.reason == "operand_width"
        assert caught.value.operation.startswith("composition_envelope_argument")


def test_original_mapping_exception_identity_is_not_replaced_or_snapshotted(monkeypatch):
    parent, _, projection, original = fixture()
    comparison = growth._NumericWork.comparison
    entered = []
    failure = RuntimeError("original later child lookup failed")

    class Original(Mapping):
        def __iter__(self):
            return iter(("items",))

        def __len__(self):
            return 1

        def __getitem__(self, key):
            if key != "items":
                raise KeyError(key)
            if entered:
                raise failure
            return 9

    original["delegates"]["leaf"][1]["dimensions"] = Original()

    def enter(work, left, right, operation):
        if operation == "composition_envelope_argument":
            entered.append(True)
        return comparison(work, left, right, operation)

    monkeypatch.setattr(growth._NumericWork, "comparison", enter)
    with pytest.raises(RuntimeError) as caught:
        validate_roots(parent, projection, original)
    assert caught.value is failure and entered


def test_receiving_batch_validates_actual_heterogeneous_closure_and_releases_it():
    parent, leaf, _, _ = fixture()
    batch = SampleBatch(
        (parent.operation,), limit=3, numeric_policy=POLICY, include_descendants=True
    )
    with batch:
        children = [
            batch.sample(leaf.operation, {"items": value}, {"work": value}) for value in (4, 9)
        ]
        batch.sample(
            parent.operation, {"calls": 2, "ceiling": 10}, {"work": 13}, {"leaf": children}
        )
    assert batch.validated_samples == 3 and batch.validation_calls == 1
    assert batch.state == "closed" and not batch._pending and not batch._source


def test_receiving_batch_does_not_turn_rejected_child_into_partial_success():
    parent, leaf, _, _ = fixture()
    batch = SampleBatch(
        (parent.operation,), limit=3, numeric_policy=POLICY, include_descendants=True
    )
    with pytest.raises(ValueError, match="envelope|composition"):
        with batch:
            children = [
                batch.sample(leaf.operation, {"items": value}, {"work": value}) for value in (4, 11)
            ]
            batch.sample(
                parent.operation, {"calls": 2, "ceiling": 10}, {"work": 15}, {"leaf": children}
            )
    assert batch.validation_attempts == 1 and batch.validation_calls == 0
    assert batch.validated_samples == 0 and batch.state == "closed"


def test_evidence_consumer_accepts_complete_exact_receipt_and_canonical_reordering():
    parent, _, projection, original = fixture()
    source, record, usage = evidence(parent, projection, original)
    reordered = {
        **projection,
        "declarations": dict(reversed(list(projection["declarations"].items()))),
    }
    assert canonical_json_bytes(reordered) == canonical_json_bytes(projection)
    for value in (projection, reordered, json.loads(canonical_json_bytes(projection))):
        assert (
            validate_growth_execution(value, record, source=source, numeric_policy=POLICY) == usage
        )


@pytest.mark.parametrize("location", ["policy", "receipt"])
def test_evidence_consumer_rejects_an_original_receipt_from_another_reader(location):
    parent, _, projection, original = fixture()
    source, record, _ = evidence(parent, projection, original)
    selected = (
        record["numeric_policy"]
        if location == "policy"
        else record["numeric_validation"]["numeric_policy"]
    )
    selected["reader_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="reader|numeric work receipt"):
        validate_growth_execution(projection, record, source=source, numeric_policy=POLICY)


def test_evidence_consumer_rejects_changed_envelope_and_preserves_original_observations():
    parent, _, projection, original = fixture()
    source, record, _ = evidence(parent, projection, original)
    before = canonical_json_bytes(record)
    altered = copy.deepcopy(projection)
    altered["declarations"][parent.declaration.id]["bounds"]["work"]["expression"]["op"] = (
        "delegate"
    )
    with pytest.raises(ValueError, match="normative declaration.*stale"):
        validate_growth_execution(altered, record, source=source, numeric_policy=POLICY)
    assert canonical_json_bytes(record) == before
