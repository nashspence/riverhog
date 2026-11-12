from __future__ import annotations

import inspect
import sys
import traceback
from collections import defaultdict

import pytest
from riverhog_core.services.physical_execution import (
    PhysicalWorkspaceExecutionKey,
    _PhysicalExecutionResult,
)

# Import the source owner to install its composed declarations.
from riverhog_core.services.upload_journal_validation_retirement import (
    UploadJournalValidationRetirement,
)
from riverhog_growth_contracts import growth_binding

from tests.support.journal_retirement_async import exception_shape, sample
from tests.unit.test_archive_upload_execution import _joined
from tests.unit.test_archive_upload_execution import owned as owned

OPERATION = _PhysicalExecutionResult.release_exception_frames


def failure(depth):
    def raise_at(remaining):
        if remaining:
            raise_at(remaining - 1)
        raise RuntimeError("original execution diagnostic")

    try:
        raise_at(depth)
    except RuntimeError as error:
        return error


@pytest.mark.growth_witness(
    "native-retirement.joined-exception-graph",
    gates=("unit",),
    operation=OPERATION,
)
def test_exception_graph_is_counted_after_join_not_as_constant_progress(
    owned, monkeypatch, record_property
):
    assert UploadJournalValidationRetirement.advance is not None
    samples = []
    lines, start = inspect.getsourcelines(OPERATION)
    append_line = start + next(
        i for i, line in enumerate(lines) if "pending.append(chained)" in line
    )
    extend_line = start + next(
        i for i, line in enumerate(lines) if "pending.extend(current.exceptions)" in line
    )
    for count in (1, 17, 129):
        for depth in (1, 5):
            nodes = [failure(depth) for _ in range(count)]
            for left, right in zip(nodes, nodes[1:], strict=False):
                left.__cause__ = right
            nodes[-1].__context__ = nodes[0]
            original = ExceptionGroup("original branches", [*nodes, nodes[0]])
            pool = owned.make(terminal_workers=1)
            identity, owner = object(), object()
            key = PhysicalWorkspaceExecutionKey("a" * 32, "b" * 64)

            def work(original=original):
                raise original

            assert (
                pool.try_submit_workspace(
                    key, target_incarnation="original", owner=owner, identity=identity, work=work
                )
                == "submitted"
            )
            _joined(pool)
            (outcome,) = pool.collect_workspace(limit=1, owner=owner)
            assert outcome.identity is identity and outcome._future.exception() is original
            n, e, f = exception_shape(outcome)
            observed = defaultdict(int)
            clear_frames = traceback.clear_frames

            def clear(tb, observed=observed, clear_frames=clear_frames):
                observed["clear_frames_calls"] += 1
                current = tb
                while current is not None:
                    observed["traceback_frame_visits"] += 1
                    current = current.tb_next
                return clear_frames(tb)

            def trace(frame, event, arg, observed=observed):
                if frame.f_code is OPERATION.__code__ and event == "line":
                    if frame.f_lineno == append_line:
                        observed["graph_edges"] += 1
                    if frame.f_lineno == extend_line:
                        observed["graph_edges"] += len(frame.f_locals["current"].exceptions)
                return trace

            with monkeypatch.context() as patch:
                patch.setattr(traceback, "clear_frames", clear)
                before = sys.gettrace()
                try:
                    sys.settrace(trace)
                    outcome.release_exception_frames()
                finally:
                    sys.settrace(before)
            assert observed["graph_edges"] == e
            samples.append(
                sample(
                    OPERATION,
                    {
                        "exception_nodes": n,
                        "exception_edges": e,
                        "traceback_frames": f,
                    },
                    observed,
                    {},
                )
            )
            assert outcome._future.exception() is original
            assert not pool.has_unreceived_work()
    assert len(samples) == 6
    assert growth_binding(OPERATION).declaration.id == samples[0]["declaration"]
    record_property("growth_samples", samples)
