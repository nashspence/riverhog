from __future__ import annotations

import hashlib
from collections.abc import Iterable

from riverhog_canonical_json import canonical_json_bytes

from riverhog_protocol.artifact_identity import ArtifactId, ArtifactMemberIdentityDocument


class ArtifactSetIdentityBuilder:
    """Seal a nonempty, strictly ID-ordered member stream with bounded state."""

    def __init__(self) -> None:
        self._digest = hashlib.sha256(b'{"artifacts":[')
        self._previous: ArtifactId | None = None
        self._sealed: str | None = None
        self.count = 0
        self.bytes = 0

    def add(self, member: ArtifactMemberIdentityDocument) -> None:
        if self._sealed is not None:
            raise ValueError("artifact set is sealed")
        if self._previous is not None and member.artifact_id <= self._previous:
            raise ValueError("artifact IDs must be strictly increasing")
        if self.count:
            self._digest.update(b",")
        self._digest.update(canonical_json_bytes(member.model_dump(mode="json")))
        self._previous = member.artifact_id
        self.count += 1
        self.bytes += member.bytes

    def finish(self) -> str:
        if not self.count:
            raise ValueError("artifact set must be nonempty")
        if self._sealed is None:
            self._digest.update(b'],"format":"riverhog-artifact-set/v1"}')
            self._sealed = self._digest.hexdigest()
        return self._sealed


def artifact_set_identity_ordered(
    members: Iterable[ArtifactMemberIdentityDocument],
) -> str:
    builder = ArtifactSetIdentityBuilder()
    for member in members:
        builder.add(member)
    return builder.finish()


def artifact_set_identity(members: Iterable[ArtifactMemberIdentityDocument]) -> str:
    return artifact_set_identity_ordered(sorted(members, key=lambda item: item.artifact_id))


__all__ = [
    "ArtifactSetIdentityBuilder",
    "artifact_set_identity",
    "artifact_set_identity_ordered",
]
