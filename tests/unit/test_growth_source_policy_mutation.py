from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import replace

import pytest
import riverhog_growth_contracts as growth


def _source():
    declaration = growth.Declaration(
        id="fixture.source-policy-mutation.growth/v1",
        authority="fixture",
        subjects=("fixture.source_policy_mutation",),
        scope="successful-call-primitive-work",
        starts_at="accept input",
        ends_at="return exact output",
        dimensions=(growth.Dimension("items", growth.COUNT, "Observed items."),),
        bounds=(
            growth.Bound(
                "visits",
                growth.COUNT,
                "equal",
                growth.Expression("dimension", name="items"),
                "Observed visits.",
            ),
        ),
        conditions=("Finite complete observations.",),
        invariants=("Every original observation is checked.",),
        limitations=("No elapsed-time promise.",),
    )

    @growth.declare_growth(declaration)
    def operation():
        return 1

    return growth.growth_binding(operation)


@pytest.mark.parametrize("field", ["dimensions", "bounds"])
def test_source_expansion_cannot_restore_its_shape_after_unpaid_table_work(field):
    binding = _source()
    declaration = binding.declaration
    original = getattr(declaration, field)
    events = []

    class RestoringName(str):
        def __hash__(self):
            events.append("replacement-table-hash")
            object.__setattr__(declaration, field, original)
            return str.__hash__(self)

    if field == "dimensions":
        names = ("items", *(f"spare_{index}" for index in range(63)))
        replacement = tuple(
            growth.Dimension(
                RestoringName(name) if index == 0 else name,
                growth.COUNT,
                "An observed source dimension.",
            )
            for index, name in enumerate(names)
        )
    else:
        names = ("items",)
        replacement = tuple(
            growth.Bound(
                RestoringName("visits") if index == 0 else f"other_{index}",
                growth.COUNT,
                "equal",
                growth.Expression("dimension", name="items"),
                "An observed source metric.",
            )
            for index in range(64)
        )

    class Observations(Mapping[str, int]):
        def __iter__(self) -> Iterator[str]:
            if field == "dimensions":
                events.append("original-name-scan")
                object.__setattr__(declaration, field, replacement)
            return iter(names)

        def __len__(self) -> int:
            return len(names)

        def __getitem__(self, key: str) -> int:
            if key not in names:
                raise KeyError(key)
            return 1

        def values(self):
            if field == "bounds":
                events.append("original-value-scan")
                object.__setattr__(declaration, field, replacement)
            return (1,) * len(names)

    try:
        with pytest.raises(growth.NumericWorkRefused) as caught:
            growth.evaluate_bound(
                binding,
                "visits",
                Observations(),
                numeric_policy=growth.NumericWorkPolicy(1_000_000_000, 64),
            )
    finally:
        object.__setattr__(declaration, field, original)

    assert caught.value.reason == "unsupported_changed_source_record"
    assert events == ["original-name-scan" if field == "dimensions" else "original-value-scan"]
    assert getattr(declaration, field) is original


def test_source_projection_guards_later_fields_after_original_key_callbacks():
    binding = _source()
    declaration = binding.declaration
    dimension = declaration.dimensions[0]
    original_name, original_bounds = dimension.name, declaration.bounds
    events = []

    class RestoringMetric(str):
        def __hash__(self):
            events.append("replacement-metric-hash")
            object.__setattr__(declaration, "bounds", original_bounds)
            return str.__hash__(self)

    replacement = tuple(
        growth.Bound(
            RestoringMetric("visits") if index == 0 else f"other_{index}",
            growth.COUNT,
            "equal",
            growth.Expression("dimension", name="items"),
            "An observed source metric.",
        )
        for index in range(64)
    )

    class ChangingDimension(str):
        def __hash__(self):
            events.append("source-dimension-hash")
            object.__setattr__(declaration, "bounds", replacement)
            return str.__hash__(self)

    object.__setattr__(dimension, "name", ChangingDimension(original_name))
    try:
        with pytest.raises(growth.NumericWorkRefused) as caught:
            growth.project_bindings(
                (binding,), numeric_policy=growth.NumericWorkPolicy(1_000_000_000, 64)
            )
    finally:
        object.__setattr__(dimension, "name", original_name)
        object.__setattr__(declaration, "bounds", original_bounds)

    assert caught.value.reason == "unsupported_changed_source_record"
    assert events == ["source-dimension-hash"]


def test_source_projection_guards_pending_delegates_after_original_key_callbacks():
    child = _source()
    events = []
    armed = False

    class ChangingDelegate(str):
        def __hash__(self):
            if armed:
                events.append("original-delegate-hash")
                object.__setattr__(parent, "delegates", replacement)
            return str.__hash__(self)

    declaration = replace(
        child.declaration,
        id="fixture.source-policy-parent.growth/v1",
        subjects=("fixture.source_policy_parent",),
    )

    @growth.declare_growth(declaration, delegates=((ChangingDelegate("leaf"), child.operation),))
    def operation():
        return 1

    parent = growth.growth_binding(operation)
    original = parent.delegates

    class RestoringOperation:
        def __call__(self):
            return 1

        def __getattribute__(self, name):
            if name == "__riverhog_growth__":
                events.append("replacement-source-lookup")
                object.__setattr__(parent, "delegates", original)
            return object.__getattribute__(self, name)

    replacement_operation = RestoringOperation()
    replacement_operation.__riverhog_growth__ = growth.Binding(
        child.declaration, replacement_operation, ()
    )
    replacement = tuple((f"injected_{index}", replacement_operation) for index in range(64))
    armed = True
    try:
        with pytest.raises(growth.NumericWorkRefused) as caught:
            growth.project_bindings(
                (parent,), numeric_policy=growth.NumericWorkPolicy(1_000_000_000, 64)
            )
    finally:
        object.__setattr__(parent, "delegates", original)

    assert caught.value.reason == "unsupported_changed_source_record"
    assert events == ["original-delegate-hash"]
