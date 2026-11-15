from __future__ import annotations

import copy
import dis
import hashlib
import sys
import weakref
from collections.abc import Mapping
from dataclasses import FrozenInstanceError

import pytest
import riverhog_growth_contracts as growth
from riverhog_canonical_json import canonical_json_bytes

from tests.unit.test_growth_literal_unit_graph import _constant, _dimension, _row


def policy(*, max_work=1_000_000, max_integer_bits=4096):
    """Explicit allowances for these tiny fixtures, not an operational default."""
    return growth.NumericWorkPolicy(max_work=max_work, max_integer_bits=max_integer_bits)


def projection(*rows):
    return {
        "format": growth.FORMAT,
        "cost_model": growth.COST_MODEL,
        "declarations": {row["id"]: row for row in rows},
    }


def sample(row, *, items=3, metric="visits", value=3, delegates=None):
    return {
        "declaration": row["id"],
        "declaration_sha256": hashlib.sha256(canonical_json_bytes(row)).hexdigest(),
        "dimensions": {"items": items},
        "metrics": {metric: value},
        "delegates": {} if delegates is None else delegates,
    }


def leaf():
    row = _row("numeric_budget_leaf")
    original = sample(row)
    return projection(row), [(row["id"], [original])]


def validate(data, groups, *, allowance=None):
    return growth.validate_execution_roots(
        canonical_json_bytes(data),
        groups,
        numeric_policy=policy() if allowance is None else allowance,
    )


def legacy_validate(data, groups):
    """Test-only old semantic control; no public unbounded reader route exists."""
    literal = growth.parse_identity_json(canonical_json_bytes(data))
    contexts = growth._projection_contexts(literal, _literal_units=True)
    check = growth._execution_root_checker(contexts, literal["declarations"])
    for identity, originals in groups:
        for original in originals:
            check(identity, original, 0, [65536])


def call(name, *, argument=None, metric="visits", multiplicity=None):
    return {
        "op": "delegate",
        "name": name,
        "metric": metric,
        "multiplicity": _constant(value=0) if multiplicity is None else multiplicity,
        "arguments": {"items": _dimension() if argument is None else argument},
    }


def shared_graph(levels, *, distinct=False):
    rows = [_row("numeric_budget_0")]
    for level in range(1, levels + 1):
        doubled = {
            "op": "product",
            "terms": [_constant(growth.SCALAR, 2), _dimension()],
        }
        expressions = [
            call("left", argument=doubled if distinct else None),
            call(
                "right",
                argument={"op": "sum", "terms": [doubled, _constant(value=1)]}
                if distinct
                else None,
            ),
        ]
        rows.append(
            _row(
                f"numeric_budget_{level}",
                delegates={edge: f"numeric_budget_{level - 1}" for edge in ("left", "right")},
                bounds={"visits": (growth.COUNT, "equal", {"op": "sum", "terms": expressions})},
            )
        )
    root = rows[-1]
    original = sample(
        root,
        items=1,
        value=0 if levels else 1,
        delegates={edge: [] for edge in root["delegates"]},
    )
    return projection(*rows), [(root["id"], [original])]


class LiveMapping(Mapping):
    def __init__(self, data, events, *, label="dimensions", on_get=None):
        self.data, self.events = data, events
        self.label, self.on_get = label, on_get

    def __iter__(self):
        self.events.append(self.label + ":iter")
        return iter(self.data)

    def __len__(self):
        self.events.append(self.label + ":len")
        return len(self.data)

    def __getitem__(self, name):
        self.events.append(self.label + ":get:" + str(name))
        value = self.data[name]
        return value if self.on_get is None else self.on_get(name, value)

    def values(self):
        self.events.append(self.label + ":values")
        return self.data.values()


class CallbackInt(int):
    def bit_length(self):
        raise AssertionError("policy invoked an integer-subclass method")

    def __hash__(self):
        raise AssertionError("policy hashed an unvalidated numeric operand")

    def __lt__(self, _other):
        raise AssertionError("policy compared an integer subclass")

    def __le__(self, _other):
        raise AssertionError("policy compared an integer subclass")

    def __eq__(self, _other):
        raise AssertionError("policy compared an integer subclass")

    def __ne__(self, _other):
        raise AssertionError("policy compared an integer subclass")

    def __mul__(self, _other):
        raise AssertionError("policy multiplied an integer subclass")

    def __rmul__(self, _other):
        raise AssertionError("policy multiplied an integer subclass")


@pytest.mark.parametrize("field", ["max_work", "max_integer_bits"])
@pytest.mark.parametrize("invalid", [True, False, 0, -1, 1.0, "1", None, CallbackInt(1)])
def test_policy_requires_explicit_positive_exact_integers(field, invalid):
    with pytest.raises(ValueError) as caught:
        policy(**{field: invalid})
    assert not isinstance(caught.value, growth.NumericWorkRefused)


@pytest.mark.parametrize("invalid", [object(), {}, "policy", 1, True])
def test_reader_rejects_a_non_policy_before_observing_originals(invalid):
    data, groups = leaf()
    events = []
    original = groups[0][1][0]
    original["dimensions"] = LiveMapping(original["dimensions"], events)
    with pytest.raises(ValueError) as caught:
        validate(data, groups, allowance=invalid)
    assert not isinstance(caught.value, growth.NumericWorkRefused)
    assert events == []


def test_policy_subclass_cannot_supply_callback_configuration():
    events = []

    class OtherPolicy(growth.NumericWorkPolicy):
        def __getattribute__(self, name):
            events.append(name)
            return super().__getattribute__(name)

    invalid = OtherPolicy(max_work=1000, max_integer_bits=1000)
    events.clear()
    data, groups = leaf()
    with pytest.raises(ValueError) as caught:
        validate(data, groups, allowance=invalid)
    assert type(caught.value) is ValueError
    assert events == []


@pytest.mark.parametrize("field", ["max_work", "max_integer_bits"])
def test_forced_policy_mutation_is_revalidated_before_original_observation(field):
    allowance = policy()
    object.__setattr__(allowance, field, True)
    data, groups = leaf()
    events = []
    original = groups[0][1][0]
    original["dimensions"] = LiveMapping(original["dimensions"], events)
    with pytest.raises(ValueError) as caught:
        validate(data, groups, allowance=allowance)
    assert type(caught.value) is ValueError
    assert events == []


def test_original_callback_cannot_change_the_active_policy_snapshot():
    allowance = policy()
    data, groups = leaf()
    original = groups[0][1][0]
    events = []

    def change_allowance(_name, value):
        object.__setattr__(allowance, "max_work", 1)
        object.__setattr__(allowance, "max_integer_bits", 1)
        return value

    original["dimensions"] = LiveMapping(original["dimensions"], events, on_get=change_allowance)
    assert validate(data, groups, allowance=allowance).metric_calls == 1
    prior_events = list(events)
    with pytest.raises(growth.NumericWorkRefused) as caught:
        validate(data, groups, allowance=allowance)
    assert caught.value.reason == "work_allowance_exhausted"
    assert events == prior_events


def test_policy_is_immutable_and_can_be_reused_for_independent_invocations():
    allowance = policy()
    with pytest.raises(FrozenInstanceError):
        allowance.max_work = 1
    data, groups = leaf()
    first = validate(data, groups, allowance=allowance)
    second = validate(data, groups, allowance=allowance)
    assert first == second
    assert first is not second
    assert first.consumed > 0
    assert allowance.max_work == 1_000_000


def test_meter_own_work_is_included_without_taxing_unused_allowance():
    data, groups = leaf()
    usage = validate(data, groups)
    assert usage.meter_work >= 3 * usage.debit_calls
    assert usage.consumed == usage.payload_work + usage.meter_work
    power_boundary = 1 << usage.consumed.bit_length()
    for work in (
        usage.consumed,
        power_boundary - 1,
        power_boundary,
        power_boundary + 1,
        1 << 20_000,
    ):
        assert validate(data, groups, allowance=policy(max_work=work)) == usage
    assert (
        validate(data, groups, allowance=policy(max_work=1 << 20_000, max_integer_bits=1 << 20_000))
        == usage
    )


def test_exact_work_allowance_completes_and_one_less_refuses():
    data, groups = leaf()
    usage = validate(data, groups)
    assert validate(data, groups, allowance=policy(max_work=usage.consumed)) == usage
    with pytest.raises(growth.NumericWorkRefused) as caught:
        validate(data, groups, allowance=policy(max_work=usage.consumed - 1))
    refusal = caught.value
    assert refusal.reason == "work_allowance_exhausted"
    assert 0 <= refusal.consumed <= refusal.limit == usage.consumed - 1


@pytest.mark.parametrize("separate_groups", [False, True])
def test_work_allowance_is_shared_by_all_original_occurrences(separate_groups):
    data, groups = leaf()
    one = validate(data, groups)
    original = groups[0][1][0]
    if separate_groups:
        groups.append((groups[0][0], [original]))
    else:
        groups[0][1].append(original)
    complete = validate(data, groups)
    assert complete.metric_calls == 2
    assert complete.expression_nodes == 2
    with pytest.raises(growth.NumericWorkRefused) as caught:
        validate(data, groups, allowance=policy(max_work=one.consumed))
    assert caught.value.reason == "work_allowance_exhausted"
    assert caught.value.consumed <= one.consumed


@pytest.mark.parametrize("distinct", [False, True])
@pytest.mark.parametrize("levels", [0, 1, 3])
def test_work_receipt_counts_expansion_from_stored_expressions(distinct, levels):
    data, groups = shared_graph(levels, distinct=distinct)
    usage = validate(data, groups)
    assert usage.metric_calls == 2 ** (levels + 1) - 1
    local_nodes = 13 if distinct else 7
    body_nodes = (local_nodes + 1) * 2**levels - local_nodes
    composition_evaluations = (10 if distinct else 4) if levels else 0
    assert usage.expression_nodes == body_nodes + composition_evaluations
    assert usage.composition_nodes == (local_nodes if levels else 1)
    assert usage.derived_environment_entries == 2 ** (levels + 1) - 2
    assert usage.maximum_operand_bits <= levels + 1


@pytest.mark.parametrize("distinct", [False, True])
def test_compact_zero_call_expansion_is_refused_by_explicit_work_allowance(distinct):
    data, groups = shared_graph(8, distinct=distinct)
    before = canonical_json_bytes(groups[0][1])
    with pytest.raises(growth.NumericWorkRefused) as caught:
        validate(data, groups, allowance=policy(max_work=2000))
    assert caught.value.reason == "work_allowance_exhausted"
    assert caught.value.operation not in {"policy", "plan_context", "plan_names"}
    assert caught.value.consumed <= 2000
    assert canonical_json_bytes(groups[0][1]) == before


def logarithm_graph():
    child = _row(
        "numeric_budget_log_leaf",
        bounds={"steps": (growth.SCALAR, "equal", {"op": "log2_ceil", "terms": [_dimension()]})},
    )
    parent = _row(
        "numeric_budget_log_parent",
        delegates={"child": "numeric_budget_log_leaf"},
        bounds={"steps": (growth.SCALAR, "equal", call("child", metric="steps"))},
    )
    return projection(child, parent), [
        (parent["id"], [sample(parent, items=0, metric="steps", value=0, delegates={"child": []})])
    ]


def test_admitted_zero_call_keeps_domain_error_and_refusal_is_distinguishable():
    data, groups = logarithm_graph()
    with pytest.raises(ValueError, match="growth logarithm requires a positive argument") as caught:
        validate(data, groups)
    assert type(caught.value) is ValueError
    with pytest.raises(growth.NumericWorkRefused) as refused:
        validate(data, groups, allowance=policy(max_work=1))
    assert refused.value.reason == "work_allowance_exhausted"


@pytest.mark.parametrize("invalid", [True, CallbackInt(1), [1]])
def test_nonexact_later_live_operand_is_explicitly_refused_without_value_callbacks(invalid):
    data, groups = leaf()
    original = groups[0][1][0]
    events = []
    original["dimensions"] = LiveMapping({"items": 3}, events, on_get=lambda _name, _value: invalid)
    with pytest.raises(growth.NumericWorkRefused) as caught:
        validate(data, groups)
    assert caught.value.reason == "unsupported_numeric_operand"
    assert events.count("dimensions:get:items") == 1


def test_nonexact_later_metric_comparison_is_explicitly_refused():
    data, groups = leaf()
    original = groups[0][1][0]
    events = []
    original["metrics"] = LiveMapping(
        {"visits": 3}, events, label="metrics", on_get=lambda _name, _value: CallbackInt(3)
    )
    with pytest.raises(growth.NumericWorkRefused) as caught:
        validate(data, groups)
    assert caught.value.reason == "unsupported_numeric_operand"
    assert events.count("metrics:get:visits") == 1


@pytest.mark.parametrize("route", ["sample", "literal", "bounded"])
def test_live_metric_comparison_preserves_original_conditional_truthiness(route):
    events = []

    class Probe:
        calls = 0

        def __bool__(self):
            self.calls += 1
            events.append("truth")
            return self.calls == 1

    probe = Probe()

    class ObservedMetric(int):
        def __ne__(self, _other):
            events.append("not_equal")
            return probe

    data, groups = leaf()
    original = groups[0][1][0]
    original["metrics"] = LiveMapping(
        {"visits": 3}, [], label="metrics", on_get=lambda _name, value: ObservedMetric(value)
    )
    if route == "bounded":
        with pytest.raises(growth.NumericWorkRefused) as caught:
            validate(data, groups)
        assert caught.value.reason == "unsupported_numeric_operand"
        assert events == []
    else:
        with pytest.raises(ValueError, match="actual primitive observations violate") as caught:
            if route == "sample":
                growth._validate_sample(
                    growth._projection_contexts(data)[groups[0][0]],
                    original["dimensions"],
                    original["metrics"],
                )
            else:
                legacy_validate(data, groups)
        assert type(caught.value) is ValueError
        assert events == ["not_equal", "truth"]


def live_outcome(kind, *, metered):
    class OriginalStop(BaseException):
        pass

    events = []
    marker = OriginalStop("original later argument failure")
    data, groups = shared_graph(1)
    original = groups[0][1][0]
    dimensions = original["dimensions"]
    gets = 0

    def incoming(_name, value):
        nonlocal gets
        gets += 1
        if kind in {"later-original-failure", "earlier-domain-error"} and gets == 2:
            raise marker
        if kind == "composition-reread":
            dimensions["items"] = 1
        return value

    if kind == "composition-reread":
        owner = data["declarations"][original["declaration"]]
        expression = owner["bounds"]["visits"]["expression"]["terms"][0]
        expression["multiplicity"] = _dimension()
        owner["bounds"]["visits"]["expression"] = expression
        del owner["delegates"]["right"]
        del original["delegates"]["right"]
        dimensions["items"] = 0
        original["declaration_sha256"] = hashlib.sha256(canonical_json_bytes(owner)).hexdigest()
    elif kind == "earlier-domain-error":
        data, groups = logarithm_graph()
        original = groups[0][1][0]
        dimensions = original["dimensions"]
        owner = data["declarations"][original["declaration"]]
        first = owner["bounds"]["steps"]["expression"]
        owner["bounds"]["steps"]["expression"] = {
            "op": "sum",
            "terms": [first, {"op": "log2_ceil", "terms": [_dimension()]}],
        }
        original["declaration_sha256"] = hashlib.sha256(canonical_json_bytes(owner)).hexdigest()
    elif kind in {"repeated-live-mapping", "repeated-exact-dict"}:
        data, groups = leaf()
        original = groups[0][1][0]
        dimensions = original["dimensions"]
        groups[0][1].append(original)

        def mutate_later(_name, value):
            dimensions["items"] = 4
            return value

        original["metrics"] = LiveMapping(
            original["metrics"], events, label="metrics", on_get=mutate_later
        )
    elif kind == "unused-later-dimension":
        owner = _row(
            "numeric_budget_unused_dimension",
            bounds={"visits": (growth.COUNT, "equal", _constant(value=0))},
        )
        data = projection(owner)
        original, later = sample(owner, value=0), sample(owner, value=0)
        groups = [(owner["id"], [original, later])]
        dimensions = original["dimensions"]

        def invalidate_later(_name, value):
            later["dimensions"]["items"] = True
            return value

        original["metrics"] = LiveMapping(
            original["metrics"], events, label="metrics", on_get=invalidate_later
        )

    if kind != "repeated-exact-dict":
        original["dimensions"] = LiveMapping(dimensions, events, on_get=incoming)
    try:
        if metered:
            validate(data, groups)
        else:
            legacy_validate(data, groups)
        result = {"result": "accepted"}
    except BaseException as error:
        result = {
            "result": "rejected",
            "type": type(error).__name__,
            "message": str(error),
            "original_exception_identity": error is marker,
        }
    result["events"] = events
    return result


@pytest.mark.parametrize(
    "kind, message",
    [
        ("all-incoming-and-composition-reads", None),
        ("composition-reread", "actual delegate calls contradict the declared composition"),
        ("earlier-domain-error", "growth logarithm requires a positive argument"),
        ("later-original-failure", "original later argument failure"),
        (
            "repeated-live-mapping",
            "actual primitive observations violate the declared growth bound",
        ),
        ("repeated-exact-dict", "actual primitive observations violate the declared growth bound"),
        (
            "unused-later-dimension",
            "growth dimensions must be complete nonnegative integer observations",
        ),
    ],
)
def test_admitted_numeric_work_preserves_live_callbacks_and_first_errors(kind, message):
    original = live_outcome(kind, metered=False)
    admitted = live_outcome(kind, metered=True)
    assert admitted == original
    if message is None:
        assert admitted["result"] == "accepted"
        assert admitted["events"].count("dimensions:get:items") == 4
    else:
        assert admitted["result"] == "rejected"
        assert admitted["message"] == message
        assert admitted["original_exception_identity"] == (kind == "later-original-failure")


@pytest.mark.parametrize("field, name", [("dimensions", "items"), ("metrics", "visits")])
def test_original_observation_names_keep_hash_and_comparison_callbacks(field, name):
    class Name(str):
        def __new__(cls, value, events):
            result = str.__new__(cls, value)
            result.events = events
            return result

        def __hash__(self):
            self.events.append("hash")
            return str.__hash__(self)

        def __eq__(self, other):
            self.events.append("compare")
            return str.__eq__(self, other)

    outcomes = []
    for metered in (False, True):
        events = []
        data, groups = leaf()
        groups[0][1][0][field] = {Name(name, events): 3}
        events.clear()
        if metered:
            validate(data, groups)
        else:
            legacy_validate(data, groups)
        outcomes.append(events)
    assert outcomes[0]
    assert outcomes[0] == outcomes[1]


def test_new_invocation_uses_changed_metadata_with_the_same_identities():
    data, groups = logarithm_graph()
    original = groups[0][1][0]
    child = next(row for row in data["declarations"].values() if not row["delegates"])
    invalid = copy.deepcopy(data)
    child["bounds"]["steps"]["expression"] = _constant(growth.SCALAR, 0)
    assert validate(data, groups).metric_calls == 2
    with pytest.raises(ValueError, match="growth logarithm requires a positive argument") as caught:
        validate(invalid, groups)
    assert type(caught.value) is ValueError
    assert original["metrics"] == {"steps": 0}


@pytest.mark.parametrize("field", ["dimensions", "metrics"])
@pytest.mark.parametrize("invalid", [True, -1, CallbackInt(3)])
def test_original_exact_integer_validation_keeps_its_semantic_error(field, invalid):
    data, groups = leaf()
    original = groups[0][1][0]
    original[field]["items" if field == "dimensions" else "visits"] = invalid
    with pytest.raises(ValueError) as caught:
        validate(data, groups)
    assert type(caught.value) is ValueError
    assert (
        "complete nonnegative integer" in str(caught.value)
        if field == "dimensions"
        else ("missing or malformed primitive observations" in str(caught.value))
    )


@pytest.mark.parametrize("field, name", [("dimensions", "items"), ("metrics", "visits")])
@pytest.mark.parametrize("change", ["missing", "extra"])
def test_original_environment_and_metric_names_remain_complete(field, name, change):
    data, groups = leaf()
    observed = groups[0][1][0][field]
    if change == "missing":
        del observed[name]
    else:
        observed["extra"] = 0
    with pytest.raises(ValueError) as caught:
        validate(data, groups)
    assert type(caught.value) is ValueError
    assert (
        "complete nonnegative integer" in str(caught.value)
        if field == "dimensions"
        else ("missing or malformed primitive observations" in str(caught.value))
    )


def test_result_width_is_reserved_before_the_multiplication_executes():
    expression = {"op": "product", "terms": [_dimension(), _dimension()]}
    row = _row("numeric_budget_product", bounds={"visits": (growth.SCALAR, "equal", expression)})
    row["dimensions"]["items"]["unit"] = []
    original = sample(row, items=1 << 12, value=0)
    events = []
    original["dimensions"] = LiveMapping(original["dimensions"], events)
    executed_products = []
    multiplication_lines = set()

    def trace(frame, event, _argument):
        if frame.f_code.co_filename != growth.__file__ or frame.f_code.co_name != "evaluate":
            return None
        if event == "call":
            multiplication_lines.update(
                item.positions.lineno
                for item in dis.get_instructions(frame.f_code)
                if item.opname == "BINARY_OP" and item.argrepr == "*="
            )
        elif event == "line" and frame.f_lineno in multiplication_lines:
            executed_products.append((frame.f_locals["result"], frame.f_locals["value"]))
        return trace

    previous = sys.gettrace()
    sys.settrace(trace)
    try:
        with pytest.raises(growth.NumericWorkRefused) as caught:
            validate(
                projection(row), [(row["id"], [original])], allowance=policy(max_integer_bits=16)
            )
    finally:
        sys.settrace(previous)
    assert caught.value.reason == "operation_width"
    assert events.count("dimensions:get:items") == 2
    assert executed_products == [(1, 1 << 12)]


def test_large_policy_integer_does_not_break_original_refusal_receiving():
    data, groups = leaf()
    groups[0][1][0]["dimensions"]["items"] = 1 << 12
    allowance = policy(max_work=1 << 20_000, max_integer_bits=8)
    with pytest.raises(growth.NumericWorkRefused) as caught:
        validate(data, groups, allowance=allowance)
    assert caught.value.reason == "operand_width"
    assert caught.value.limit == allowance.max_work
    assert str(caught.value) == "growth numeric work refused: operand_width"


@pytest.mark.parametrize("field", ["dimensions", "metrics"])
def test_later_live_exact_integer_width_is_checked_before_use(field):
    data, groups = leaf()
    original = groups[0][1][0]
    events = []
    original[field] = LiveMapping(
        original[field], events, label=field, on_get=lambda _name, _value: 1 << 12
    )
    with pytest.raises(growth.NumericWorkRefused) as caught:
        validate(data, groups, allowance=policy(max_integer_bits=8))
    assert caught.value.reason == "operand_width"


@pytest.mark.parametrize("operator", ["sum", "product"])
def test_supported_width_is_a_conservative_reservation_not_only_the_actual_result(operator):
    expression = {
        "op": operator,
        "terms": [_constant(growth.SCALAR, 0), _constant(growth.SCALAR, 0)],
    }
    row = _row(
        "numeric_budget_zero_result", bounds={"visits": (growth.SCALAR, "equal", expression)}
    )
    data, groups = projection(row), [(row["id"], [sample(row, value=0)])]
    assert validate(data, groups).metric_calls == 1
    with pytest.raises(growth.NumericWorkRefused) as caught:
        validate(data, groups, allowance=policy(max_integer_bits=1))
    assert caught.value.reason == "operation_width"
    assert caught.value.requested == 2


def test_private_refusal_does_not_consume_later_source_or_mapping_allowances():
    from tests.unit.test_growth_observer_batch import leaf_binding, literal_sample

    binding = leaf_binding()
    data = growth.project_bindings((binding,), numeric_policy=policy())
    originals = [literal_sample(binding)]
    with pytest.raises(growth.NumericWorkRefused):
        validate(data, [(binding.declaration.id, originals)], allowance=policy(max_work=1))
    assert growth.evaluate_bound(binding, "visits", {"items": 3}, numeric_policy=policy()) == 3
    assert (
        growth.validate_execution_samples(
            data, binding.declaration.id, originals, numeric_policy=policy()
        ).metric_calls
        == 1
    )


def test_original_mapping_scan_cost_is_explicitly_outside_private_derived_quota():
    class RepeatedValues(LiveMapping):
        def __init__(self, entries):
            super().__init__({"items": 3}, [])
            self.entries, self.seen = entries, 0

        def values(self):
            for _ in range(self.entries):
                self.seen += 1
                yield 3

    receipts = []
    for count in (1, 64):
        data, groups = leaf()
        original = RepeatedValues(count)
        groups[0][1][0]["dimensions"] = original
        receipts.append(validate(data, groups))
        assert original.seen == count
    assert receipts[0] == receipts[1]


def test_unused_invalid_metadata_is_checked_before_original_observations():
    data, groups = leaf()
    unused = _row(
        "numeric_budget_unused", bounds={"visits": (growth.COUNT, "equal", _dimension("missing"))}
    )
    data["declarations"][unused["id"]] = unused
    events = []
    original = groups[0][1][0]
    original["dimensions"] = LiveMapping(original["dimensions"], events)
    with pytest.raises(ValueError, match="unknown dimension") as caught:
        validate(data, groups)
    assert type(caught.value) is ValueError
    assert events == []


def test_original_baseexception_and_its_traceback_keep_ownership():
    class OriginalStop(BaseException):
        pass

    class WeakDict(dict):
        pass

    marker = OriginalStop("original numeric callback failure")

    class Dimensions(WeakDict):
        def __getitem__(self, _name):
            raise marker

    data, groups = leaf()
    original = WeakDict(groups[0][1][0])
    dimensions = Dimensions(items=3)
    original["dimensions"] = dimensions
    groups = [(groups[0][0], [original])]
    original_ref, dimensions_ref = weakref.ref(original), weakref.ref(dimensions)
    try:
        with pytest.raises(OriginalStop) as caught:
            validate(data, groups)
        assert caught.value is marker
        del groups, original, dimensions
        assert original_ref() is not None
        assert dimensions_ref() is not None
        assert marker.__traceback__ is not None
    finally:
        marker.__traceback__ = None


def test_refusal_traceback_retains_originals_until_its_receiver_releases_it():
    class WeakDict(dict):
        pass

    data, groups = leaf()
    original = WeakDict(groups[0][1][0])
    dimensions = WeakDict(original["dimensions"])
    original["dimensions"] = dimensions
    groups = [(groups[0][0], [original])]
    original_ref, dimensions_ref = weakref.ref(original), weakref.ref(dimensions)
    with pytest.raises(growth.NumericWorkRefused) as caught:
        validate(data, groups, allowance=policy(max_work=1))
    try:
        del groups, original, dimensions
        assert original_ref() is not None
        assert dimensions_ref() is not None
        assert caught.value.__traceback__ is not None
    finally:
        caught.value.__traceback__ = None


def test_successful_numeric_receipt_does_not_retain_originals():
    class WeakDict(dict):
        pass

    data, groups = leaf()
    original = WeakDict(groups[0][1][0])
    dimensions = WeakDict(original["dimensions"])
    original["dimensions"] = dimensions
    groups = [(groups[0][0], [original])]
    original_ref, dimensions_ref = weakref.ref(original), weakref.ref(dimensions)
    receipt = validate(data, groups)
    del groups, original, dimensions
    assert receipt.metric_calls == 1
    assert original_ref() is None
    assert dimensions_ref() is None
