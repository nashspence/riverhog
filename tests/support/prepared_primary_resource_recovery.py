"""Shared rollback of interrupted resource preparation without adopting an unsealed primary."""

from __future__ import annotations

import hashlib
import sqlite3
from unittest.mock import patch

from riverhog_client import PrimaryStateError, PrimaryStore
from riverhog_client._prepared_primary_documents import PrimarySession, primary_control_bytes
from riverhog_client._prepared_primary_store import PrimaryPreparation
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
)

from tests.support.prepared_primary_replay import ALLOCATION, DESCRIPTOR, LIMITS, SCOPE

ROLES = ("prepared", "decision-accepted", "custody", "reclaimed")
REQUEST = b"exact member preparation request"
JOURNAL = b"original retained journal" * 400


def cases():
    return (
        (role, phase, committed)
        for role in ROLES
        for phase in ("warming", "readiness")
        for committed in (False, True)
    )


class _CommitResponseFailure:
    """The real COMMIT either executes or does not; its caller always loses the response."""

    def __init__(self, connection, *, committed):
        self.connection, self.committed = connection, committed

    @property
    def in_transaction(self):
        return self.connection.in_transaction

    def execute(self, statement, *args):
        if statement == "COMMIT":
            if self.committed:
                self.connection.execute(statement).close()
            raise sqlite3.OperationalError("injected resource commit response loss")
        return self.connection.execute(statement, *args)

    def blobopen(self, *args, **kwargs):
        return self.connection.blobopen(*args, **kwargs)

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()


def _member(number):
    return ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": f"{number:064x}",
            "bytes": "7",
            "sha256": hashlib.sha256(b"payload").hexdigest(),
        }
    )


def _preparation(member):
    binding = CollectionArtifactProvenanceBindingDocument.model_validate(
        {
            "artifact_id": member.artifact_id,
            "journal": {
                "journal_id": "urn:uuid:aaaabbbb-1234-4234-8234-123456789abc",
                "through": {
                    "entry_id": "urn:uuid:aaaabbbb-4321-4321-8321-123456789abc",
                    "sequence": "0",
                    "json_sha256": "a" * 64,
                },
                "prefix_bytes": str(len(JOURNAL)),
                "prefix_sha256": hashlib.sha256(JOURNAL).hexdigest(),
            },
            "delivery_association_id": "urn:uuid:bbbbaaaa-1234-4234-8234-123456789abc",
        }
    )
    return PrimaryPreparation(
        b"original observation", b"original graph", JOURNAL, b"original decision", binding, None
    )


def _slots(files):
    return files._one(
        "SELECT count(*),sum(length(body)) FROM primary_controls WHERE namespace=? "
        "AND name IN ('prepared','decision-accepted','custody','reclaimed')",
        (files._namespace,),
    )


def witness(path, factory, *, role, phase, committed):
    from riverhog_client import _prepared_primary_sqlite_journal as journal

    survivor, interrupted = _member(1), _member(2)
    calls = 0

    def author(_grant):
        nonlocal calls
        calls += 1
        return _preparation(interrupted)

    def opened(*, initialize=False, no_preimages=False):
        def storage(location, *, allocation_descriptor, initialize):
            files = factory(
                location, allocation_descriptor=allocation_descriptor, initialize=initialize
            )
            if no_preimages:

                def authorize(action, table, *_rest):
                    return (
                        sqlite3.SQLITE_DENY
                        if action == sqlite3.SQLITE_READ and table == "primary_chunks"
                        else sqlite3.SQLITE_OK
                    )

                files._connection.set_authorizer(authorize)
            return files

        return PrimaryStore(
            path,
            allocation_id=ALLOCATION.allocation_id,
            scope=SCOPE,
            limits=LIMITS,
            initialize=initialize,
            storage_factory=storage,
        )

    with opened(initialize=True) as store:
        create = b"exact creation request"
        assert store.session_for(create) is None
        store.bind_session(
            PrimarySession(
                allocation_sha256=hashlib.sha256(DESCRIPTOR).hexdigest(),
                create_request={
                    "bytes": str(len(create)),
                    "sha256": hashlib.sha256(create).hexdigest(),
                },
                collection_id="42",
                delivery_context_id="urn:uuid:ccccaaaa-1234-4234-8234-123456789abc",
                server_construction_identity_sha256="a" * 64,
                attribution_construction_identity_sha256="b" * 64,
                completion_requirement_sha256=None,
            )
        )
        original = store.prepare_or_load(survivor, REQUEST, lambda _grant: _preparation(survivor))
        before = store.usage
        sealed = primary_control_bytes(original.prepared)
        warm = journal._warm
        injected = False

        def fail_response(files, identifier):
            nonlocal injected
            selected = files._one(
                "SELECT namespace,name FROM primary_controls WHERE rowid=?", (identifier,)
            )
            chosen = not injected and selected == (files._namespace, role) and files._namespace != 1
            if chosen:
                injected = True
                if phase == "warming":
                    files._root._connection = _CommitResponseFailure(
                        files._root._connection, committed=committed
                    )
            warm(files, identifier)
            if chosen and phase == "readiness":
                files._root._connection = _CommitResponseFailure(
                    files._root._connection, committed=committed
                )

        with patch.object(journal, "_warm", fail_response):
            try:
                store.prepare_or_load(interrupted, REQUEST, author)
            except PrimaryStateError as exc:
                assert isinstance(exc.__cause__, sqlite3.Error)
            else:
                raise AssertionError("resource commit uncertainty was reported as prepared")
        assert injected and calls == 1
        assert store._usage == before

    with opened(no_preimages=True) as store:
        assert store.usage == before
        assert not store._files.exists("transition")
        assert store.load_controls(interrupted) is None
        retained = store.load_controls(survivor)
        assert retained == original and primary_control_bytes(retained.prepared) == sealed
        with store._member(str(interrupted.artifact_id), create=False) as files:
            assert _slots(files) == (4, 4 * (LIMITS.control_bytes + 9))
            for name in ROLES:
                assert not files.exists(name)
        store._files._connection.set_authorizer(None)
        with store.byte_reader(original, "journal") as stream:
            assert stream.read() == JOURNAL
        selected = store.prepare_or_load(interrupted, REQUEST, author)
        after = store.usage
        assert calls == 2 and after.members == before.members + 1
        assert after.control_bytes == before.control_bytes + LIMITS.member_reservation
        assert after.pending_bytes == before.pending_bytes + sum(
            part.bytes for part in selected.prepared.byte_files
        )
        with store._member(str(interrupted.artifact_id), create=False) as files:
            assert _slots(files) == (4, 4 * (LIMITS.control_bytes + 9))

    def no_author(_grant):
        raise AssertionError("a selected primary was authored again")

    with opened() as store:
        assert store.usage == after
        assert store.prepare_or_load(interrupted, REQUEST, no_author) == selected
        assert store.load_controls(survivor) == original
        with store.byte_reader(selected, "journal") as stream:
            assert stream.read() == JOURNAL
    return {
        "role": role,
        "phase": phase,
        "commit_executed": committed,
        "survivor_exact": True,
        "unsealed_primary_adopted": False,
        "retry_reused_capacity": True,
        "member_grant_charged_once": True,
        "fault_kind": "injected COMMIT response loss, not VFS or power failure",
    }
