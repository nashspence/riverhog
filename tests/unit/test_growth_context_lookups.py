from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import replace

import pytest
import riverhog_growth_contracts as growth
from riverhog_canonical_json import canonical_json_bytes


def _binding(reader, name, dimensions, bounds, *, delegates=()):
    declaration = reader.Declaration(
        id=f"fixture.{name}.growth/v1",
        authority="fixture",
        subjects=(f"fixture.{name}",),
        scope="successful-call-primitive-work",
        starts_at="accept input",
        ends_at="return exact output",
        dimensions=dimensions,
        bounds=bounds,
        conditions=("Finite valid input.",),
        invariants=("Complete exact output.",),
        limitations=("No numerical elapsed-time promise.",),
    )

    def operation():
        return "exact-output"

    reader.declare_growth(declaration, delegates=delegates)(operation)
    return reader.growth_binding(operation)


def lookup_bindings(reader=growth):
    """Use the supplied public types so an independent reader can build its own sources."""
    dimension = reader.Expression
    dimensions = (
        reader.Dimension("items", reader.COUNT, "Actual items."),
        reader.Dimension("width", reader.OCTETS_PER_COUNT, "Octets per actual item."),
        reader.Dimension("spare", reader.COUNT, "An observed dimension unused by bounds."),
    )
    leaf = _binding(
        reader,
        "lookup_leaf",
        dimensions,
        (
            reader.Bound(
                "visits", reader.COUNT, "equal", dimension("dimension", name="items"), "Visits."
            ),
            reader.Bound(
                "octets",
                reader.OCTETS,
                "upper",
                dimension(
                    "product",
                    terms=(
                        dimension("dimension", name="items"),
                        dimension("dimension", name="width"),
                    ),
                ),
                "Observed octets.",
            ),
            reader.Bound(
                "steps",
                reader.SCALAR,
                "equal",
                dimension("log2_ceil", terms=(dimension("dimension", name="items"),)),
                "Binary depth.",
            ),
        ),
    )

    def call(metric):
        return dimension(
            "delegate",
            name="leaf",
            metric=metric,
            terms=(dimension("dimension", name="calls"),),
            arguments=tuple(
                (item.name, dimension("dimension", name=item.name)) for item in dimensions
            ),
        )

    parent = _binding(
        reader,
        "lookup_parent",
        (*dimensions, reader.Dimension("calls", reader.COUNT, "Actual child calls.")),
        (
            reader.Bound("visits", reader.COUNT, "equal", call("visits"), "Actual visits."),
            reader.Bound("octets", reader.OCTETS, "upper", call("octets"), "Observed octets."),
        ),
        delegates=(("leaf", leaf.operation),),
    )
    return parent, leaf


def _projection(*bindings):
    return {
        "format": growth.FORMAT,
        "cost_model": growth.COST_MODEL,
        "declarations": {binding.declaration.id: binding.project() for binding in bindings},
    }


def _sample(projection, identity, dimensions, metrics, *, delegates=None):
    return {
        "declaration": identity,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][identity])
        ).hexdigest(),
        "dimensions": copy.deepcopy(dimensions),
        "metrics": copy.deepcopy(metrics),
        "delegates": {} if delegates is None else copy.deepcopy(delegates),
    }


def apply_context_changes(projection, action):
    """Apply explicit JSON mutations between public calls, retaining projection identity."""
    for change in action.get("changes", ()):
        target = projection
        for name in change["path"][:-1]:
            target = target[name]
        target[change["path"][-1]] = copy.deepcopy(change["value"])


def invoke_context_action(reader, projection, action):
    """Dispatch public calls without interpreting declarations or expected results."""
    entrypoint = action["entrypoint"]
    if entrypoint == "validate_projection":
        return reader.validate_projection(projection)
    if entrypoint == "validate_sample":
        return reader.validate_sample(
            projection, action["declaration_id"], action["dimensions"], action["metrics"]
        )
    if entrypoint == "validate_execution_samples":
        return reader.validate_execution_samples(
            projection, action["declaration_id"], action["samples"]
        )
    raise AssertionError(entrypoint)


def _action(entrypoint, expected="accept", **values):
    return {"entrypoint": entrypoint, "expected": expected, **values}


def _case(name, projection, *actions):
    return copy.deepcopy({"name": name, "projection": projection, "actions": list(actions)})


def context_lookup_cases():
    """Small JSON cases shared by owning tests and an external public-reader comparison."""
    parent, leaf = lookup_bindings()
    leaf_id, parent_id = leaf.declaration.id, parent.declaration.id
    leaf_projection = _projection(leaf)
    projection = _projection(parent, leaf)
    dimensions = {"items": 3, "width": 4, "spare": 0}
    metrics = {"visits": 3, "octets": 12, "steps": 2}
    parent_dimensions = {**dimensions, "calls": 2}
    parent_metrics = {"visits": 6, "octets": 24}
    leaf_sample = _sample(projection, leaf_id, dimensions, metrics)
    parent_sample = _sample(
        projection,
        parent_id,
        parent_dimensions,
        parent_metrics,
        delegates={"leaf": [copy.deepcopy(leaf_sample), copy.deepcopy(leaf_sample)]},
    )

    def checks(current, identity=leaf_id, observed_dimensions=None, observed_metrics=None):
        actual_dimensions = dimensions if observed_dimensions is None else observed_dimensions
        actual_metrics = metrics if observed_metrics is None else observed_metrics
        return (
            _action("validate_projection"),
            _action(
                "validate_sample",
                declaration_id=identity,
                dimensions=actual_dimensions,
                metrics=actual_metrics,
            ),
            _action(
                "validate_execution_samples",
                declaration_id=identity,
                samples=[_sample(current, identity, actual_dimensions, actual_metrics)],
            ),
        )

    yield _case(
        "literal-arithmetic-and-complete-unused-dimension",
        leaf_projection,
        *checks(leaf_projection),
    )
    yield _case(
        "literal-delegated-observations",
        projection,
        _action("validate_projection"),
        _action(
            "validate_sample",
            declaration_id=parent_id,
            dimensions=parent_dimensions,
            metrics=parent_metrics,
        ),
        _action("validate_execution_samples", declaration_id=parent_id, samples=[parent_sample]),
    )

    separate = copy.deepcopy(projection)
    count_id, octet_id = "fixture.lookup_count.growth/v1", "fixture.lookup_octet.growth/v1"
    count_row = copy.deepcopy(separate["declarations"].pop(leaf_id))
    count_row.update(id=count_id, subjects=["fixture.lookup_count"])
    count_row["dimensions"] = {"items": count_row["dimensions"]["items"]}
    count_row["bounds"] = {"visits": count_row["bounds"]["visits"]}
    octet_row = copy.deepcopy(count_row)
    octet_row.update(id=octet_id, subjects=["fixture.lookup_octet"])
    octet_row["dimensions"]["items"]["unit"] = [["octets", 1]]
    octet_row["bounds"]["visits"]["unit"] = [["octets", 1]]
    separate["declarations"].update({count_id: count_row, octet_id: octet_row})
    separate_parent = separate["declarations"][parent_id]
    separate_parent["dimensions"] = {
        "counts": copy.deepcopy(count_row["dimensions"]["items"]),
        "bytes": copy.deepcopy(octet_row["dimensions"]["items"]),
    }
    separate_parent["bounds"], separate_parent["delegates"] = {}, {}
    separate_children = {}
    for name, child_id, child_row, observed in (
        ("counts", count_id, count_row, 3),
        ("bytes", octet_id, octet_row, 11),
    ):
        bound = copy.deepcopy(child_row["bounds"]["visits"])
        bound["expression"] = {
            "op": "delegate",
            "name": name,
            "metric": "visits",
            "multiplicity": {"op": "constant", "value": 1, "unit": [["count", 1]]},
            "arguments": {"items": {"op": "dimension", "name": name}},
        }
        separate_parent["bounds"][name] = bound
        separate_parent["delegates"][name] = {"declaration": child_id}
        separate_children[name] = [
            _sample(separate, child_id, {"items": observed}, {"visits": observed})
        ]
    separate_sample = _sample(
        separate,
        parent_id,
        {"counts": 3, "bytes": 11},
        {"counts": 3, "bytes": 11},
        delegates=separate_children,
    )
    yield _case(
        "same-names-have-context-specific-units-and-bounds",
        separate,
        _action("validate_projection"),
        _action("validate_execution_samples", declaration_id=parent_id, samples=[separate_sample]),
    )

    for field, name, value in (
        ("dimensions", "missing", None),
        ("dimensions", "extra", 1),
        ("dimensions", "bool", True),
        ("dimensions", "float", 3.0),
        ("dimensions", "negative", -1),
        ("metrics", "missing", None),
        ("metrics", "extra", 1),
        ("metrics", "bool", True),
        ("metrics", "float", 3.0),
        ("metrics", "negative", -1),
    ):
        observed_dimensions, observed_metrics = dict(dimensions), dict(metrics)
        target = observed_dimensions if field == "dimensions" else observed_metrics
        selected = "spare" if field == "dimensions" else "steps"
        if name == "missing":
            target.pop(selected)
        else:
            target["extra" if name == "extra" else selected] = value
        actions = checks(
            leaf_projection,
            observed_dimensions=observed_dimensions,
            observed_metrics=observed_metrics,
        )[1:]
        for action in actions:
            action["expected"] = "reject"
        yield _case(f"complete-{field}-{name}", leaf_projection, *actions)

    for field in ("dimensions", "metrics"):
        bad = copy.deepcopy(leaf_sample)
        bad[field] = list(bad[field].items())
        yield _case(
            "actual-mapping-" + field,
            leaf_projection,
            _action("validate_execution_samples", "reject", declaration_id=leaf_id, samples=[bad]),
        )

    row_path = ["declarations", leaf_id]
    malformed = (
        ("unknown-top-field", ["unknown"], 1),
        (
            "dimension-map-shape",
            [*row_path, "dimensions"],
            [["items", {"unit": [["count", 1]], "meaning": "Items."}]],
        ),
        ("bound-map-shape", [*row_path, "bounds"], []),
        ("unknown-dimension-field", [*row_path, "dimensions", "items", "unknown"], 0),
        ("unknown-bound-field", [*row_path, "bounds", "visits", "unknown"], 0),
        (
            "duplicate-unit-name",
            [*row_path, "dimensions", "items", "unit"],
            [["count", 1], ["count", 1]],
        ),
        ("metric-mode", [*row_path, "bounds", "visits", "mode"], "average"),
        ("expression-extra-field", [*row_path, "bounds", "visits", "expression", "hidden"], 0),
        ("unknown-dimension", [*row_path, "bounds", "visits", "expression", "name"], "absent"),
        ("owner-key", [*row_path, "id"], "fixture.foreign.growth/v1"),
    )
    for name, path, value in malformed:
        changed = copy.deepcopy(leaf_projection)
        apply_context_changes(changed, {"changes": [{"path": path, "value": value}]})
        actions = checks(changed)
        for action in actions:
            action["expected"] = "reject"
        yield _case(name, changed, *actions)

    unused_id = "fixture.lookup_unused.growth/v1"
    with_unused = copy.deepcopy(leaf_projection)
    unused = copy.deepcopy(with_unused["declarations"][leaf_id])
    unused.update(id=unused_id, subjects=["fixture.lookup_unused"])
    with_unused["declarations"][unused_id] = unused
    for name, path, value in (
        ("unit", ["bounds", "visits", "unit"], [["octets", 1]]),
        ("unknown-field", ["dimensions", "items", "hidden"], 1),
        ("unresolved-edge", ["delegates"], {"unused": {"declaration": "fixture.absent.growth/v1"}}),
        ("duplicate-subject", ["subjects"], ["fixture.lookup_leaf"]),
        ("cyclic-edge", ["delegates"], {"unused": {"declaration": unused_id}}),
    ):
        changed = copy.deepcopy(with_unused)
        apply_context_changes(
            changed, {"changes": [{"path": ["declarations", unused_id, *path], "value": value}]}
        )
        actions = checks(changed)
        for action in actions:
            action["expected"] = "reject"
        yield _case("invalid-unused-" + name, changed, *actions)

    for parent_relation in ("equal", "upper", "big_o"):
        for child_relation in ("equal", "upper", "big_o"):
            changed = copy.deepcopy(projection)
            changed["declarations"][parent_id]["bounds"]["visits"]["relation"] = parent_relation
            changed["declarations"][leaf_id]["bounds"]["visits"]["relation"] = child_relation
            accepted = (
                parent_relation == "big_o"
                or child_relation == "equal"
                or (parent_relation == "upper" and child_relation == "upper")
            )
            yield _case(
                f"relation-{parent_relation}-{child_relation}",
                changed,
                _action("validate_projection", "accept" if accepted else "reject"),
            )

    shared = copy.deepcopy(projection)
    row = shared["declarations"][parent_id]
    first = copy.deepcopy(row["bounds"]["visits"])
    first["relation"] = "upper"
    first["expression"]["name"] = "first"
    second = copy.deepcopy(first)
    second["expression"]["name"] = "second"
    second["relation"] = "equal"
    row["bounds"] = {"first_visits": first, "second_visits": second}
    row["delegates"] = {name: {"declaration": leaf_id} for name in ("first", "second")}
    shared_sample = _sample(
        shared,
        parent_id,
        parent_dimensions,
        {"first_visits": 6, "second_visits": 6},
        delegates={
            name: [copy.deepcopy(leaf_sample), copy.deepcopy(leaf_sample)]
            for name in ("first", "second")
        },
    )
    yield _case(
        "shared-child-valid-through-both-edges",
        shared,
        _action("validate_projection"),
        _action("validate_execution_samples", declaration_id=parent_id, samples=[shared_sample]),
    )
    for defect in ("count", "arguments"):
        bad_sample = copy.deepcopy(shared_sample)
        if defect == "count":
            bad_sample["delegates"]["second"].pop()
        else:
            child = bad_sample["delegates"]["second"][-1]
            child["dimensions"]["items"] = 4
            child["metrics"].update(visits=4, octets=16, steps=2)
        yield _case(
            "shared-child-actual-" + defect,
            shared,
            _action(
                "validate_execution_samples",
                "reject",
                declaration_id=parent_id,
                samples=[bad_sample],
            ),
        )
    for defect in ("relation", "mapping", "metric", "multiplicity", "argument-relation"):
        for reverse in (False, True):
            changed = copy.deepcopy(shared)
            bounds = changed["declarations"][parent_id]["bounds"]
            expression = bounds["second_visits"]["expression"]
            if defect == "relation":
                changed["declarations"][leaf_id]["bounds"]["visits"]["relation"] = "upper"
            elif defect == "mapping":
                expression["arguments"]["items"] = {"op": "dimension", "name": "width"}
            elif defect == "metric":
                expression["metric"] = "absent"
            elif defect == "multiplicity":
                expression["multiplicity"] = {"op": "constant", "value": 0, "unit": []}
            else:
                changed["declarations"][leaf_id]["bounds"]["visits"]["relation"] = "upper"
                bounds["second_visits"]["relation"] = "upper"
                expression["arguments"]["items"] = copy.deepcopy(
                    bounds["first_visits"]["expression"]
                )
            if reverse:
                changed["declarations"][parent_id]["bounds"] = dict(reversed(tuple(bounds.items())))
            yield _case(
                f"shared-edge-{defect}-{reverse}", changed, _action("validate_projection", "reject")
            )

    zero_dimensions = {**parent_dimensions, "calls": 0}
    zero_sample = _sample(
        projection, parent_id, zero_dimensions, {"visits": 0, "octets": 0}, delegates={"leaf": []}
    )
    yield _case(
        "zero-calls-still-have-a-complete-declaration",
        projection,
        _action("validate_execution_samples", declaration_id=parent_id, samples=[zero_sample]),
    )
    zero_invalid = copy.deepcopy(projection)
    zero_invalid["declarations"][leaf_id]["bounds"]["steps"]["unit"] = [["count", 1]]
    yield _case(
        "zero-calls-do-not-hide-an-unused-invalid-bound",
        zero_invalid,
        _action(
            "validate_execution_samples", "reject", declaration_id=parent_id, samples=[zero_sample]
        ),
    )

    for entrypoint in ("validate_sample", "validate_execution_samples"):
        changed = copy.deepcopy(leaf_projection)
        doubled = {"op": "sum", "terms": [{"op": "dimension", "name": "items"}] * 2}
        change = {"path": [*row_path, "bounds", "visits", "expression"], "value": doubled}
        apply_context_changes(changed, {"changes": [change]})
        changed_metrics = {**metrics, "visits": 6}
        original_action = checks(leaf_projection)[1 if entrypoint == "validate_sample" else 2]
        rejected = checks(changed)[1 if entrypoint == "validate_sample" else 2]
        rejected.update(expected="reject", changes=[change])
        accepted = checks(changed, observed_metrics=changed_metrics)[
            1 if entrypoint == "validate_sample" else 2
        ]
        yield _case(
            "fresh-call-" + entrypoint, leaf_projection, original_action, rejected, accepted
        )

    altered_unit = {"path": [*row_path, "dimensions", "width", "unit"], "value": [["count", 1]]}
    restored_unit = {**altered_unit, "value": [["count", -1], ["octets", 1]]}
    yield _case(
        "fresh-call-projection-unit-check",
        leaf_projection,
        _action("validate_projection"),
        _action("validate_projection", "reject", changes=[altered_unit]),
        _action("validate_projection", changes=[restored_unit]),
    )
    changed_metadata = copy.deepcopy(leaf_projection)
    metadata_change = {
        "path": [*row_path, "conditions"],
        "value": ["A changed finite-input condition."],
    }
    apply_context_changes(changed_metadata, {"changes": [metadata_change]})
    yield _case(
        "fresh-call-rechecks-declaration-digest",
        leaf_projection,
        _action("validate_execution_samples", declaration_id=leaf_id, samples=[leaf_sample]),
        _action(
            "validate_execution_samples",
            "reject",
            declaration_id=leaf_id,
            samples=[leaf_sample],
            changes=[metadata_change],
        ),
        _action(
            "validate_execution_samples",
            declaration_id=leaf_id,
            samples=[_sample(changed_metadata, leaf_id, dimensions, metrics)],
        ),
    )

    renamed = copy.deepcopy(leaf_projection)
    row = renamed["declarations"][leaf_id]
    row["dimensions"]["members"] = row["dimensions"].pop("items")
    row["bounds"]["visits"]["expression"]["name"] = "members"
    row["bounds"]["steps"]["expression"]["terms"][0]["name"] = "members"
    row["bounds"]["octets"]["expression"]["terms"][0]["name"] = "members"
    row["bounds"]["observed_visits"] = row["bounds"].pop("visits")
    renamed_dimensions = {"members": 3, "width": 4, "spare": 0}
    renamed_metrics = {"observed_visits": 3, "octets": 12, "steps": 2}
    yield _case(
        "same-id-independent-dimension-and-metric-names",
        renamed,
        *checks(renamed, observed_dimensions=renamed_dimensions, observed_metrics=renamed_metrics),
        _action(
            "validate_sample",
            "reject",
            declaration_id=leaf_id,
            dimensions=dimensions,
            metrics=metrics,
        ),
    )

    last_bad = copy.deepcopy(leaf_sample)
    last_bad["metrics"]["visits"] = 4
    yield _case(
        "late-root-is-still-checked",
        leaf_projection,
        _action(
            "validate_execution_samples",
            "reject",
            declaration_id=leaf_id,
            samples=[leaf_sample, leaf_sample, last_bad],
        ),
    )
    nested_bad = copy.deepcopy(parent_sample)
    nested_bad["delegates"]["leaf"][-1]["metrics"]["steps"] = 3
    yield _case(
        "late-child-unreferenced-metric-is-still-checked",
        projection,
        _action(
            "validate_execution_samples", "reject", declaration_id=parent_id, samples=[nested_bad]
        ),
    )

    for depth in (64, 65):
        chain = copy.deepcopy(leaf_projection)
        chain["declarations"] = {}
        for index in range(depth):
            identity = f"fixture.lookup_depth_{index}.growth/v1"
            row = copy.deepcopy(leaf_projection["declarations"][leaf_id])
            row.update(id=identity, subjects=[f"fixture.lookup_depth_{index}"])
            if index + 1 < depth:
                row["delegates"] = {
                    "unused": {"declaration": f"fixture.lookup_depth_{index + 1}.growth/v1"}
                }
            chain["declarations"][identity] = row
        for reverse in (False, True):
            current = copy.deepcopy(chain)
            if reverse:
                current["declarations"] = dict(reversed(tuple(current["declarations"].items())))
            yield _case(
                f"declared-depth-{depth}-{reverse}",
                current,
                _action("validate_projection", "accept" if depth == 64 else "reject"),
            )
        deep_expression = {"op": "dimension", "name": "items"}
        for _ in range(depth):
            deep_expression = {
                "op": "sum",
                "terms": [{"op": "dimension", "name": "items"}, deep_expression],
            }
        current = copy.deepcopy(leaf_projection)
        current["declarations"][leaf_id]["bounds"]["visits"]["expression"] = deep_expression
        yield _case(
            f"expression-depth-{depth}",
            current,
            _action("validate_projection", "accept" if depth == 64 else "reject"),
        )


@pytest.mark.parametrize("case", tuple(context_lookup_cases()), ids=lambda case: case["name"])
def test_context_lookups_preserve_public_reader_behavior(case):
    projection = copy.deepcopy(case["projection"])
    for action in case["actions"]:
        apply_context_changes(projection, action)
        before = json.dumps([projection, action], sort_keys=True, allow_nan=False)
        if action["expected"] == "reject":
            with pytest.raises(ValueError):
                invoke_context_action(growth, projection, action)
        else:
            assert invoke_context_action(growth, projection, action) is None
        assert json.dumps([projection, action], sort_keys=True, allow_nan=False) == before


def source_lookup_case(reader, defect):
    """Describe a between-call source mutation without sharing types between readers."""
    parent, leaf = lookup_bindings(reader)
    if defect == "parent-dimension":
        target, field = parent.declaration, "dimensions"
        replacement = tuple(
            replace(item, unit=reader.COUNT) if item.name == "width" else item
            for item in target.dimensions
        )
    elif defect == "child-dimension":
        target, field = leaf.declaration, "dimensions"
        replacement = tuple(
            replace(item, unit=reader.COUNT) if item.name == "width" else item
            for item in target.dimensions
        )
    elif defect == "child-bound":
        target, field = leaf.declaration, "bounds"
        replacement = tuple(item for item in target.bounds if item.metric != "visits")
    else:
        raise AssertionError(defect)
    return {
        "binding": parent,
        "projection": _projection(parent, leaf),
        "dimensions": {"items": 3, "width": 4, "spare": 0, "calls": 2},
        "metrics": {"visits": 6, "octets": 24},
        "target": target,
        "field": field,
        "original": getattr(target, field),
        "replacement": replacement,
    }


@pytest.mark.parametrize("defect", ["parent-dimension", "child-dimension", "child-bound"])
def test_source_contexts_stay_fresh_and_separate_from_parsed_metadata(defect):
    case = source_lookup_case(growth, defect)
    binding, projection = case["binding"], case["projection"]
    before = canonical_json_bytes(projection)
    assert growth.evaluate_bound(binding, "visits", case["dimensions"]) == 6
    try:
        object.__setattr__(case["target"], case["field"], case["replacement"])
        with pytest.raises(ValueError):
            growth.evaluate_bound(binding, "visits", case["dimensions"])
        growth.validate_sample(
            projection, binding.declaration.id, case["dimensions"], case["metrics"]
        )
        assert canonical_json_bytes(projection) == before
    finally:
        object.__setattr__(case["target"], case["field"], case["original"])
    assert growth.evaluate_bound(binding, "visits", case["dimensions"]) == 6


def test_projected_sources_preserve_exact_ownership_and_alias_deduplication():
    _, leaf = lookup_bindings()
    _, other = lookup_bindings()
    assert leaf.operation is not other.operation and leaf.project() == other.project()
    assert growth.project_bindings((leaf, leaf)) == _projection(leaf)
    with pytest.raises(ValueError, match="multiple source owners"):
        growth.project_bindings((leaf, other))
