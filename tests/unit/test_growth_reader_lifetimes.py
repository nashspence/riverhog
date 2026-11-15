from __future__ import annotations

import gc
import sys
import types
import weakref
from contextlib import contextmanager

import pytest
import riverhog_growth_contracts as growth
from riverhog_canonical_json import canonical_json_bytes

from tests.unit.test_growth_numeric_evaluation import (
    NUMERIC_CALLBACK_EXPECTATIONS,
    numeric_callback_outcome,
)
from tests.unit.test_growth_observer_batch import (
    graph_binding,
    graph_bound,
    graph_call,
    leaf_binding,
    literal_sample,
)


class WeakDict(dict):
    pass


@contextmanager
def profile_calls(callback):
    previous = sys.getprofile()
    sys.setprofile(callback)
    try:
        yield
    finally:
        sys.setprofile(previous)


def invoke(projection, identity, samples, *, literal):
    if literal:
        growth.validate_execution_roots(canonical_json_bytes(projection), [(identity, samples)])
    else:
        growth.validate_execution_samples(projection, identity, samples)


@pytest.mark.parametrize("literal", [False, True])
def test_successful_reader_scopes_release_recursive_functions_and_originals(literal):
    leaf = leaf_binding()
    projection = growth.project_bindings((leaf,))
    dimensions = WeakDict(items=3)
    original = WeakDict(literal_sample(leaf))
    original["dimensions"] = dimensions
    original_ref, dimensions_ref = weakref.ref(original), weakref.ref(dimensions)
    captures = []
    locals_by_owner = {
        "_projection_contexts": "visit",
        "_execution_root_checker": "check",
        "_evaluate_expression": "evaluate",
        "check": "composition",
    }

    def profile(frame, event, _arg):
        if event != "return" or frame.f_code.co_filename != growth.__file__:
            return
        name = locals_by_owner.get(frame.f_code.co_name)
        function = frame.f_locals.get(name) if name is not None else None
        if not isinstance(function, types.FunctionType):
            return
        self_cells = [
            name
            for name, cell in zip(
                function.__code__.co_freevars, function.__closure__ or (), strict=True
            )
            if cell.cell_contents is function
        ]
        captures.append((function.__name__, self_cells, weakref.ref(function)))

    with profile_calls(profile):
        invoke(projection, leaf.declaration.id, [original], literal=literal)
    assert {name for name, _, _ in captures} == {"visit", "check", "evaluate", "composition"}
    assert all(not cells for _, cells, _ in captures)
    assert all(reference() is None for _, _, reference in captures)
    del original, dimensions
    assert original_ref() is None
    assert dimensions_ref() is None


@pytest.mark.parametrize("literal", [False, True])
def test_original_exception_owns_its_traceback_and_original_values(literal):
    leaf = leaf_binding()
    projection = growth.project_bindings((leaf,))

    class OriginalStop(BaseException):
        pass

    marker = OriginalStop("original callback failure")
    calls = []

    class Dimensions(WeakDict):
        def __getitem__(self, name):
            calls.append(name)
            raise marker

    dimensions = Dimensions(items=3)
    original = WeakDict(literal_sample(leaf))
    original["dimensions"] = dimensions
    original_ref, dimensions_ref = weakref.ref(original), weakref.ref(dimensions)
    try:
        with pytest.raises(OriginalStop) as caught:
            invoke(projection, leaf.declaration.id, [original], literal=literal)
        assert caught.value is marker
        names = []
        traceback = marker.__traceback__
        while traceback is not None:
            if traceback.tb_frame.f_code.co_filename == growth.__file__:
                names.append(traceback.tb_frame.f_code.co_name)
            traceback = traceback.tb_next
        assert names == [
            "validate_execution_roots" if literal else "validate_execution_samples",
            "check",
            "_validate_sample",
            "_evaluate",
            "_evaluate_expression",
            "evaluate",
        ]
        assert calls == ["items"]
        del original, dimensions
        assert original_ref() is not None
        assert dimensions_ref() is not None
    finally:
        # This test is the receiver of the original failure; runtime never
        # clears a traceback to shorten its source-owned resource lifetime.
        marker.__traceback__ = None


def test_reader_does_not_request_or_configure_collection(monkeypatch):
    leaf = leaf_binding()
    projection = growth.project_bindings((leaf,))
    before = gc.isenabled(), gc.get_threshold(), gc.get_debug()

    def forbidden(*_args, **_kwargs):
        raise AssertionError("reader changed collector policy or requested a collection")

    for name in (
        "collect",
        "disable",
        "enable",
        "freeze",
        "unfreeze",
        "set_debug",
        "set_threshold",
    ):
        monkeypatch.setattr(gc, name, forbidden)
    growth.validate_execution_roots(
        canonical_json_bytes(projection), [(leaf.declaration.id, [literal_sample(leaf)])]
    )
    assert (gc.isenabled(), gc.get_threshold(), gc.get_debug()) == before


@pytest.mark.parametrize("literal", [False, True])
def test_recursive_checker_keeps_direct_python_frames(literal):
    current = leaf_binding()
    original = literal_sample(current)
    for index in range(16):
        parent = graph_binding(
            f"lifetime_parent_{index}",
            graph_bound(graph_call("child")),
            delegates=(("child", current.operation),),
        )
        original = literal_sample(parent, children={"child": [original]})
        current = parent
    projection = growth.project_bindings((current,))
    callers = []

    def profile(frame, event, _arg):
        if (
            event == "call"
            and frame.f_code.co_filename == growth.__file__
            and frame.f_code.co_name == "check"
        ):
            callers.append(frame.f_back.f_code.co_name)

    with profile_calls(profile):
        invoke(projection, current.declaration.id, [original], literal=literal)
    assert callers == [
        "validate_execution_roots" if literal else "validate_execution_samples",
        *(["check"] * 16),
    ]


@pytest.mark.parametrize("name, expected", NUMERIC_CALLBACK_EXPECTATIONS)
def test_recursion_ownership_preserves_mapping_and_source_callbacks(name, expected):
    result = numeric_callback_outcome(growth, name)
    if isinstance(expected, str):
        assert result == {
            "kind": "error",
            "type": "ValueError",
            "message": expected,
            "source_restored": True,
        }
    else:
        assert result == {"kind": "return", "value": expected, "source_restored": True}
