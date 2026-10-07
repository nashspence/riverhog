"""Operational membership index over immutable facts, independent of recipe roles."""

from stove0_protocol import WorkArtifactSubject, canonical_json_sha256


def subject_identity_sha256(subject: WorkArtifactSubject) -> str:
    # Classification changes how this work uses a member. It does not change
    # which member an accepted observation describes. This index is not a new
    # archive identity or evidence record.
    return canonical_json_sha256(subject.model_dump(mode="json", exclude={"role"}))


def artifact_identity_sha256(subject: WorkArtifactSubject) -> str:
    """An operational lookup of the existing Riverhog member identity."""
    return canonical_json_sha256(subject.model_dump(mode="json", exclude={"id", "role"}))
