"""Executable integration illustration against the pinned Native/Core source.

This is a TEST BRIDGE, not a drop-in product patch or optional runtime mode.
The hard cut is specified in INTEGRATION.md. Context routing lets the experiment
exercise unchanged Native schema/session code while replacing its raw origins.
It never imports Core from the neutral installed package.
"""
from __future__ import annotations

import contextvars
import os
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

from retained_resource.construction import Construction
from sqlite_admission import SqliteConnectionAdmission
from riverhog_provenance import journal_store
from riverhog_provenance.journal_store import InvalidJournalWorkspace, JournalStore


class DedicatedSqliteScope:
    """Reference application scope using the existing independent SQLite owner.

    Exactly one construction owns this admission. The production equivalent can
    instead be a per-attempt ticket from the application's shared admission; it
    must not use a shared aggregate zero to certify an unrelated construction.
    Grants below are explicitly supplied by the fixture, not product defaults.
    """
    def __init__(self, *, cursor_limit, verify_process):
        self.admission = SqliteConnectionAdmission(
            connection_limit=1, cursor_limit=cursor_limit, operation_limit=1,
            verify_process=verify_process,
        )

    def connect(self, database, **options):
        return self.admission.connect(database, **options)

    def close(self):
        for original in self.admission.pending_connections:
            original.close()

    @property
    def received(self):
        status = self.admission.status
        return not (status.connections or status.cursors or status.blobs or status.operations)


class NativeStorageReceipt:
    def __init__(self, store):
        self.store = store

    def close(self):
        self.store.close()

    @property
    def received(self):
        # This is the Native owner's own final-reader predicate. The neutral
        # file receiver never inspects it or claims to certify SQLite handles.
        return self.store._storage._closed


_active = contextvars.ContextVar("reference_original_constructor", default=None)


def open_retained(cls, directory, *, create=False, read_only=False,
                  on_storage_close=None, on_storage_open=None, connection_factory=None):
    frame = _active.get()
    if frame is None:
        raise RuntimeError("test bridge requires the caller-retained original constructor")
    if connection_factory is not None:
        raise RuntimeError("test bridge cannot replace an independent caller's connection scope")
    frame.enter()
    try:
        if create and read_only:
            raise ValueError("read-only retained creation is invalid")
        directory = Path(directory)
        if directory.is_symlink() or not directory.is_dir():
            raise InvalidJournalWorkspace("retained journal directory is unavailable")
        path = directory / "snapshot.sqlite3"
        if path.is_symlink():
            raise InvalidJournalWorkspace("retained journal database must be owner-created")
        store = cls.__new__(cls)  # no resource acquisition
        if create:
            frame.create_exclusive(path)
            frame.sync_directory(directory)
        elif not path.is_file():
            raise InvalidJournalWorkspace("retained journal database is missing")

        def retain(actual):
            frame.retain_storage(NativeStorageReceipt(actual))
            if on_storage_open is not None:
                on_storage_open(actual)

        store._open(directory, retained=True, reopen=not create, read_only=read_only,
                    on_storage_open=retain, connection_factory=frame.connect)
        frame.ready()
        store._storage.on_close = on_storage_close
        return store
    finally:
        frame.leave()


def sync_directory(directory):
    frame = _active.get()
    if frame is None:
        raise RuntimeError("no retained namespace-sync owner")
    frame.sync_directory(Path(directory))


@contextmanager
def native_route(frame):
    token = _active.set(frame)
    try:
        yield
    finally:
        _active.reset(token)


@contextmanager
def native_bridge():
    with patch.object(JournalStore, "open_retained", classmethod(open_retained)), patch.object(
        journal_store, "_sync_workspace_directory", sync_directory
    ):
        yield


@contextmanager
def core_bridge(frame_factory):
    """Patch only the test binding; exercise actual advance/lease/session/schema.

    The original Core algorithm otherwise stays unchanged. The real integration
    adds these fields/receiving checks directly and updates all maintained callers.
    """
    from riverhog_core.services import upload_journal_validation_owner as core
    original_open, original_drop = core.UploadJournalValidationOwner._open, core.UploadJournalValidationOwner._drop

    @dataclass(slots=True)
    class Slot(core._Slot):
        construction: object = None

    class RetainingStack(ExitStack):
        def __init__(self, frame):
            super().__init__()
            self.frame = frame

        def close(self):
            # Database closure is positive before Native's final-reader callback;
            # that callback precedes Native's final _closed bookkeeping assignment.
            if not self.frame.dependents_received:
                raise BlockingIOError("original constructor dependents remain unreceived")
            super().close()

    def opening(owner, source, slot, catalog):
        if slot.construction is None:
            frame = frame_factory()
            slot.construction = frame  # before namespace, file, SQLite or validation
            slot.resources = RetainingStack(frame)
        with native_route(slot.construction):
            return original_open(owner, source, slot, catalog)

    def dropping(owner, key, slot):
        slot.closing = True
        frame = slot.construction
        if frame is not None and not frame.close_step():
            raise BlockingIOError("original Native constructor receiving remains pending")
        return original_drop(owner, key, slot)

    with native_bridge(), patch.object(core, "_Slot", Slot), patch.object(
        core.UploadJournalValidationOwner, "_open", opening
    ), patch.object(core.UploadJournalValidationOwner, "_drop", dropping):
        yield
