"""Test-owned original lease frames through actual context and reference receiving."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar

from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_core.journal_validation_workspace_lease import ValidationWorkspaceLease

_receivers: ContextVar[list[ValidationWorkspaceLease] | None] = ContextVar(
    "test_validation_workspace_receivers", default=None
)


@contextmanager
def retained_workspace_frames():
    frames: list[ValidationWorkspaceLease] = []
    token = _receivers.set(frames)
    try:
        yield frames
    finally:
        _receivers.reset(token)
        for original in reversed(frames):
            original.close()


@contextmanager
def leased_workspace(root, identity, **options):
    original = ValidationWorkspaceLease()
    receivers = _receivers.get()
    if receivers is not None:
        receivers.append(original)
    try:
        with lease_journal_validation_workspace(
            root, identity, receiver=original, **options
        ) as path:
            yield path
    finally:
        if receivers is None:
            original.close()
