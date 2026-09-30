from __future__ import annotations

import pytest
from pydantic import ValidationError
from riverhog_protocol.provenance_transport import ArtifactMaterializationDecisionDocument

_ARTIFACT_ID = "a" * 64


def test_publication_decision_requires_exactly_one_supplied_choice() -> None:
    hinted = ArtifactMaterializationDecisionDocument.model_validate(
        {"artifact_id": _ARTIFACT_ID, "materialization_hint": {"components": ["clip.mkv"]}}
    )
    assert hinted.materialization_hint is not None
    assert hinted.materialization_hint.components == ["clip.mkv"]
    omitted = ArtifactMaterializationDecisionDocument.model_validate(
        {"artifact_id": _ARTIFACT_ID, "allow_missing_materialization_hint": True}
    )
    assert omitted.materialization_hint is None
    for invalid in (
        {"artifact_id": _ARTIFACT_ID},
        {
            "artifact_id": _ARTIFACT_ID,
            "materialization_hint": {"components": ["clip.mkv"]},
            "allow_missing_materialization_hint": True,
        },
        {"artifact_id": _ARTIFACT_ID, "allow_missing_materialization_hint": "true"},
        {
            "artifact_id": _ARTIFACT_ID,
            "materialization_hint": None,
            "allow_missing_materialization_hint": True,
        },
        {"artifact_id": _ARTIFACT_ID, "materialization_hint": {"components": [".."]}},
    ):
        with pytest.raises(ValidationError):
            ArtifactMaterializationDecisionDocument.model_validate(invalid)
