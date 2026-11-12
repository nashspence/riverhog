"""Retain actual test-owned namespace resources through complete receiving."""

from __future__ import annotations

import os
from contextlib import contextmanager

import pytest
from riverhog_core import journal_validation_retirement as owner
from riverhog_core.retained_workspace_handles import (
    RetainedWorkspaceHandles,
    RetainedWorkspaceResource,
)


@contextmanager
def lease_retiring_journal_workspace(root, identity, *, handles=None):
    handles = RetainedWorkspaceHandles() if handles is None else handles
    try:
        with owner.lease_retiring_journal_workspace(root, identity, handles=handles) as selected:
            yield selected
    finally:
        handles.close()


def remove_retired_journal_workspace_directory(root, identity, *, handles=None):
    handles = RetainedWorkspaceHandles() if handles is None else handles
    try:
        return owner.remove_retired_journal_workspace_directory(root, identity, handles=handles)
    finally:
        handles.close()


@contextmanager
def observe_retained_namespace(recorder, *, before_acquire=None):
    originals = {}
    with pytest.MonkeyPatch.context() as patch:
        for name in ("acquire", "close"):
            actual = getattr(RetainedWorkspaceResource, name)

            def observed(handle, *args, _actual=actual, _acquiring=name == "acquire", **kwargs):
                before = handle.owner.metrics
                active = recorder.active
                try:
                    if active and _acquiring and before_acquire is not None:
                        before_acquire()
                    return _actual(handle, *args, **kwargs)
                finally:
                    if active:
                        after = handle.owner.metrics
                        for source, metric in (
                            ("open_calls", "open_calls"),
                            ("close_calls", "close_calls"),
                            ("fstat_calls", "stat_calls"),
                            ("dup_calls", "dup_calls"),
                            ("control_octets", "native_control_octets"),
                        ):
                            recorder.metrics[metric] += after[source] - before[source]
                        if _acquiring:
                            recorder.metrics["path_octets"] += len(os.fsencode(args[0]))
                        if handle.borrow.active:
                            originals[handle] = handle.fileno()
                        if handle.owner.received or handle.owner.state == "absent":
                            recorder.live.discard(handle)
                            if handle in originals:
                                recorder.live.discard(originals[handle])
                        elif handle.owner.state in {"opening", "live", "rejected", "unknown"}:
                            recorder.live.add(handle)
                            if handle in originals:
                                recorder.live.add(originals[handle])

            patch.setattr(RetainedWorkspaceResource, name, observed)
        yield


@contextmanager
def refuse_native_child_resource(patch, inode, enabled, *, message, lease):
    acquire, close = RetainedWorkspaceResource.acquire, RetainedWorkspaceResource.close
    selected, triggered, active = set(), [], [True]

    def acquiring(handle, path, **kwargs):
        result = acquire(handle, path, **kwargs)
        parent = kwargs.get("parent")
        target = parent if lease else handle
        if (
            target is not None
            and (not lease or path == ".lease")
            and os.fstat(target.fileno()).st_ino == inode
        ):
            selected.add(handle)
        return result

    def closing(handle):
        if active[0] and enabled() and handle in selected:
            triggered.append(handle)
            raise OSError(message)
        return close(handle)

    patch.setattr(RetainedWorkspaceResource, "acquire", acquiring)
    patch.setattr(RetainedWorkspaceResource, "close", closing)
    try:
        yield triggered
    finally:
        active[0] = False


@contextmanager
def native_namespace_close_fault(patch, native, *, parent_inode, name, after):
    from riverhog_core import retained_workspace_handles as resources

    actual = resources.RetainedWorkspaceResource.acquire
    selected, descriptors = [], []

    def acquiring(handle, path, **kwargs):
        parent = kwargs.get("parent")
        chosen = (
            path == name and parent is not None and os.fstat(parent.fileno()).st_ino == parent_inode
        )
        if chosen:
            selected.append(handle)
            handle.owner._test_fault("after_close" if after else "before_close")
        result = actual(handle, path, **kwargs)
        if chosen:
            descriptors.append(handle.fileno())
        return result

    patch.setattr(resources, "Resource", native.Resource)
    patch.setattr(resources, "Borrow", native.Borrow)
    patch.setattr(resources.RetainedWorkspaceResource, "acquire", acquiring)
    yield selected, descriptors
