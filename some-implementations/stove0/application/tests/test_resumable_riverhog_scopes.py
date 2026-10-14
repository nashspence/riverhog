"""Exact generic scopes advance in bounded pages across restarts and lost ACKs."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from riverhog_protocol import Conflict
from riverhog_protocol.collection_workflow_transport import (
    ArtifactReceivingSetDocument,
    ProcessingCapabilityDocument,
)
from riverhog_protocol.collection_workflows import canonical_json_bytes
from sqlalchemy import select
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_core.riverhog import Stove0RiverhogClient
from stove0_core.work_state import ClaimBinding
from stove0_protocol import ArtifactSelection, CollectionRootIdentityRef, WorkArtifactSubject


class ScopeApi:
    def __init__(self):
        self.fence = 1
        self.members = {}
        self.calls = []
        self.lose_ack = True

    def create_processing_capability(self, claim_id, **kwargs):
        assert kwargs["artifacts"] is None
        key = f"{len(self.members) + 1:032x}"
        self.members[key] = []
        self.calls.append(("create", 0))
        return self._capability(claim_id, key, kwargs["audience"], "receiving")

    def _capability(self, claim_id, key, audience, state):
        return ProcessingCapabilityDocument.model_validate(
            {
                "format": "riverhog-processing-capability/v1",
                "id": key,
                "claim_id": claim_id,
                "fence": str(self.fence),
                "audience": audience,
                "actions": ["read-inputs"],
                "state": state,
                "principal_id": f"claim:{claim_id}",
                "expires_at": "2099-01-01T00:00:00.000000000Z",
                "artifacts": self._status(key, sealed=state == "active"),
                "token": "rhc_transient-bearer-fixture",
            }
        )

    def _status(self, key, *, sealed=False):
        values = self.members.get(key, [])
        digest = hashlib.sha256(b"riverhog-claim-artifacts/v1\0")
        for item in values:
            data = canonical_json_bytes(item)
            digest.update(len(data).to_bytes(8, "big"))
            digest.update(data)
        count, total = len(values), sum(int(item["bytes"]) for item in values)
        return ArtifactReceivingSetDocument.model_validate(
            {
                "state": "sealed" if sealed else "receiving",
                "count": str(count),
                "total_bytes": str(total),
                "identity": {
                    "count": str(count),
                    "total_bytes": str(total),
                    "sha256": digest.hexdigest(),
                }
                if sealed
                else None,
            }
        )

    def append_processing_capability_artifacts(self, claim_id, key, **kwargs):
        assert kwargs["fence"] == self.fence
        page = kwargs["artifacts"]
        self.calls.append(("append", len(page)))
        ordinal = kwargs["start_ordinal"]
        values = self.members[key]
        for item in page:
            if ordinal < len(values):
                assert canonical_json_bytes(values[ordinal]) == canonical_json_bytes(item)
            else:
                assert ordinal == len(values)
                values.append(item)
            ordinal += 1
        if self.lose_ack:
            self.lose_ack = False
            raise TimeoutError("remote append committed before its acknowledgement was lost")
        return self._status(key)

    def seal_processing_capability_artifacts(self, claim_id, key, **kwargs):
        if kwargs["fence"] != self.fence:
            raise Conflict("stale claim fence")
        self.calls.append(("seal", 0))
        return self._status(key, sealed=True)

    def refresh_processing_capability(self, claim_id, key, **kwargs):
        if kwargs["fence"] != self.fence:
            raise Conflict("stale claim fence")
        self.calls.append(("refresh", 0))
        return self._capability(claim_id, key, "stove0.observer/example", "active")


def _selection():
    roots = {
        id: CollectionRootIdentityRef(
            collection_id=str(id),
            archive_root_sha256=str(id) * 64,
            artifact_set_identity=str(id + 2) * 64,
        )
        for id in (1, 2)
    }
    return ArtifactSelection.seal(
        tuple(
            WorkArtifactSubject(
                id=f"member-{1024 - index:08d}",
                role="example.input/v1",
                collection=roots[1 + index % 2],
                artifact_id=f"{index:064x}",
                bytes="3",
                sha256="e" * 64,
            )
            for index in range(1, 132)
        )
    )


def test_capability_pages_resume_without_replaying_members_or_retaining_bearers(tmp_path: Path):
    url = f"sqlite:///{tmp_path / 'control.db'}"
    state = SqlAlchemyStateStore(url)
    selection = _selection()
    state.retain_selection(selection)
    claim = ClaimBinding(claim_id="a" * 64, fence=1)
    api = ScopeApi()
    capability = None
    for _ in range(30):
        # A new process sees only the exact durable cursor and prefix hash.
        state.engine.dispose()
        state = SqlAlchemyStateStore(url)
        adapter = Stove0RiverhogClient(
            api,
            state=state,
            declared_workspace_protection="memory-backed",
            authority_batch_size=33,
        )
        previous_calls = len(api.calls)
        try:
            capability = adapter._capability(
                claim,
                audience="stove0.observer/example",
                actions=("read-inputs",),
                scope=selection.ref(),
                owner_kind="work",
                owner_id="b" * 64,
            )
        except TimeoutError:
            pass
        assert len(api.calls) - previous_calls <= 1
        if capability is not None:
            break
    assert capability is not None
    expected = sorted(
        selection.artifacts, key=lambda item: (item.collection.collection_id, item.artifact_id)
    )
    actual = api.members[capability.id]
    assert [(int(item["collection"]["collection_id"]), item["artifact_id"]) for item in actual] == [
        (item.collection.collection_id, item.artifact_id) for item in expected
    ]
    assert len(actual) == 131
    assert max(size for _, size in api.calls) == 33
    scoped = state.planning_context("work", "b" * 64)
    with state.engine.connect() as connection:
        rows = connection.execute(select(scoped.riverhog_transfers.table)).mappings().all()
    assert len(rows) == 1
    assert all("rhc_" not in row["checkpoint_json"] for row in rows)
    assert all("rhc_" not in row["binding_json"] for row in rows)
    # Refresh uses the sealed set; neither members nor their hash are re-uploaded.
    assert (
        adapter._capability(
            claim,
            audience="stove0.observer/example",
            actions=("read-inputs",),
            scope=selection.ref(),
            owner_kind="work",
            owner_id="b" * 64,
        )
        is not None
    )
    assert api.calls[-1] == ("refresh", 0)
    api.fence = 2
    with pytest.raises(Conflict):
        adapter._capability(
            claim,
            audience="stove0.observer/example",
            actions=("read-inputs",),
            scope=selection.ref(),
            owner_kind="work",
            owner_id="b" * 64,
        )
    state.engine.dispose()
