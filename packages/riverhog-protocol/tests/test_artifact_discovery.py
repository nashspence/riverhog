from __future__ import annotations

import pytest
from pydantic import ValidationError
from riverhog_protocol import ArtifactDiscoveryRequest, AssertionClause, ProfilePin, ValuePredicate


def test_discovery_identity_is_jcs_and_predicates_are_correlated() -> None:
    query = ArtifactDiscoveryRequest.model_validate(
        {
            "collections": ["12"],
            "tags_all": ["camera"],
            "provenance_all": [
                {
                    "profile": {
                        "contract_id": "urn:example:camera",
                        "contract_sha256": "a" * 64,
                        "schema_id": "urn:example:camera-schema",
                    },
                    "values": [
                        {"pointer": "/data/camera", "operator": "equals", "value": "A"},
                        {"pointer": "/data/serial", "operator": "equals", "value": "B"},
                    ],
                }
            ],
        }
    )
    assert query.collections == ("12",)
    assert query.provenance_all[0].values[1].pointer == "/data/serial"
    assert (
        query.identity()
        == ArtifactDiscoveryRequest.model_validate(query.model_dump(mode="json")).identity()
    )


@pytest.mark.parametrize(
    "invalid",
    [
        {"pointer": "$.data.name", "value": "x"},
        {"pointer": "/bad~2escape", "value": "x"},
        {"value": "x" * 257},
        {"value": "é" * 257},
        {"operator": "bytes-equals", "value": "YQ"},
        {"operator": "bytes-equals", "value": "YQ==", "text_mode": "ascii-fold"},
        {"operator": "contains", "value": 23},
        {"operator": "equals", "value": 23, "text_mode": "ascii-fold"},
    ],
)
def test_discovery_rejects_nonliteral_or_unbounded_predicates(invalid: dict[str, object]) -> None:
    with pytest.raises((ValidationError, ValueError)):
        ValuePredicate.model_validate(invalid)


def test_discovery_rejects_ambiguous_selectors_and_scope() -> None:
    with pytest.raises(ValidationError):
        ArtifactDiscoveryRequest.model_validate({"collections": ["01"]})
    with pytest.raises(ValidationError):
        ArtifactDiscoveryRequest.model_validate({"tags_all": ["camera", "camera"]})
    with pytest.raises(ValidationError):
        AssertionClause.model_validate({"scopes": ["member", "member"], "values": [{"value": "x"}]})
    with pytest.raises(ValidationError):
        ArtifactDiscoveryRequest.model_validate({"provenance_all": [{"values": []}]})
    with pytest.raises(ValidationError):
        ProfilePin.model_validate(
            {"contract_id": "x", "contract_sha256": "A" * 64, "schema_id": "s"}
        )
