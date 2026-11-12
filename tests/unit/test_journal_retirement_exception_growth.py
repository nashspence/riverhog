from __future__ import annotations

import inspect
import sys
import traceback
from collections import defaultdict

import pytest
from riverhog_core.services import physical_execution as physical
from riverhog_core.services.upload_journal_validation_retirement import (
    UploadJournalValidationRetirement,
)

from tests.unit.test_archive_upload_execution import _joined
from tests.unit.test_archive_upload_execution import owned as owned
from tests.unit.test_upload_journal_validation_retirement_growth import sample

OPERATION = physical.release_exception_graph


def failure(depth):
    def raise_at(remaining):
        if remaining:
            raise_at(remaining - 1)
        raise RuntimeError("original execution diagnostic")

    try:
        raise_at(depth)
    except RuntimeError as error:
        return error


def shape(error):
    pending, seen, edges, frames = [error], set(), 0, 0
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        cursor = current.__traceback__
        while cursor is not None:
            frames += 1
            cursor = cursor.tb_next
        outgoing = [
            value for value in (current.__cause__, current.__context__) if value is not None
        ]
        if isinstance(current, BaseExceptionGroup):
            outgoing.extend(current.exceptions)
        edges += len(outgoing)
        pending.extend(outgoing)
    return len(seen), edges, frames


@pytest.mark.growth_witness(
    "native-retirement.original-exception-graph",
    gates=("unit",),
    operation=OPERATION,
)
def test_exception_graph_counts_cycles_shared_edges_and_depth(owned, monkeypatch, record_property):
    assert UploadJournalValidationRetirement.advance is not None
    samples = []
    lines, start = inspect.getsourcelines(OPERATION)
    pop_line = start + next(
        index for index, line in enumerate(lines) if "current = pending.pop()" in line
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
            key = physical.PhysicalWorkspaceExecutionKey("a" * 32, "b" * 64)

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
            nodes_count, edges, frames = shape(original)
            observed = defaultdict(int)
            actual_clear = traceback.clear_frames

            def clear(tb, observed=observed, actual_clear=actual_clear):
                observed["frame_clear_calls"] += 1
                cursor = tb
                while cursor is not None:
                    observed["traceback_frames"] += 1
                    cursor = cursor.tb_next
                return actual_clear(tb)

            def trace(frame, event, arg, observed=observed):
                if (
                    frame.f_code is OPERATION.__code__
                    and event == "line"
                    and frame.f_lineno == pop_line
                ):
                    observed["exception_visits"] += 1
                return trace

            with monkeypatch.context() as patch:
                patch.setattr(traceback, "clear_frames", clear)
                before = sys.gettrace()
                try:
                    sys.settrace(trace)
                    outcome.release_exception_frames()
                finally:
                    sys.settrace(before)
            samples.append(
                sample(
                    OPERATION,
                    {
                        "exception_nodes": nodes_count,
                        "exception_edges": edges,
                        "traceback_frames": frames,
                    },
                    observed,
                    {},
                )
            )
            assert observed["exception_visits"] == edges + 1
            assert observed["frame_clear_calls"] == nodes_count
            assert observed["traceback_frames"] == frames
            assert outcome._future.exception() is original
            assert not pool.has_unreceived_work()
    assert len(samples) == 6
    record_property("growth_samples", samples)
