from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest
import stove0_protocol
from stove0_observer_protocol import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    CollectionRootIdentityRef,
    ContentObservationRequest,
    JsonSchemaValidationProfile,
    ObserverContract,
    ObserverContractPayload,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    SemanticValidationProfile,
    SemanticValidationProfilePayload,
    WorkArtifactSubject,
    validate_observation_request,
)
from stove0_protocol import models as shared_models

from tests.stove0_observation_fixtures import fixture_interface, observation_payload

_OBSERVER_AUTHOR_SYMBOLS = frozenset(
    {
        "ContentObservationEvidence",
        "ContentObservationFailure",
        "ContentObservationInapplicable",
        "ContentObservationInvocation",
        "ContentObservationRequest",
        "ContentObservationRequestPayload",
        "ContentObservationResult",
        "ContentObservationResultPayload",
        "ContentObservationState",
        "ObserverContract",
        "ObserverContractPayload",
        "ObserverContractSupport",
        "ObserverDescriptor",
        "ObserverDescriptorPayload",
        "ObserverImplementation",
        "ObserverRuntimeAuthority",
        "SemanticFactsConformanceVector",
        "SemanticFactsConformanceVectors",
        "SemanticValidatorBinding",
        "SemanticValidatorProvider",
        "SemanticValidatorRegistry",
        "accept_observation_result",
        "require_semantic_validators",
        "validate_observation_request",
    }
)


def test_observer_contract_models_are_importable_without_runtime_support() -> None:
    contract = ObserverContract.seal(
        ObserverContractPayload(
            id="fixture.observation/v1",
            options_schema=JsonSchemaValidationProfile.from_schema(
                "fixture.options/v1",
                {"type": "object", "additionalProperties": False},
            ),
            facts_schema=JsonSchemaValidationProfile.from_schema(
                "fixture.facts/v1",
                {"type": "object", "additionalProperties": False},
            ),
            facts_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
        )
    )
    assert contract.contract_sha256
    assert callable(validate_observation_request)

    forbidden = {
        "httpx",
        "riverhog_client",
        "riverhog_client.processing",
        "stove0_core",
        "stove0_observer_support",
    }
    code = (
        "import sys\n"
        "import stove0_observer_protocol\n"
        f"forbidden = {forbidden!r}\n"
        "loaded = forbidden & set(sys.modules)\n"
        "assert not loaded, sorted(loaded)\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_observer_read_authority_is_explicit_and_contract_bound() -> None:
    shared = {
        "id": "fixture.observation/v1",
        "options_schema": JsonSchemaValidationProfile.from_schema(
            "fixture.options/v1", {"type": "object"}
        ),
        "facts_schema": JsonSchemaValidationProfile.from_schema(
            "fixture.facts/v1", {"type": "object"}
        ),
        "facts_semantics": JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    }
    payload = ObserverContract.seal(
        ObserverContractPayload(**shared, read_actions=("read-inputs",))
    )
    provenance = ObserverContract.seal(
        ObserverContractPayload(**shared, read_actions=("read-provenance",))
    )
    assert payload.contract_sha256 != provenance.contract_sha256
    assert ObserverContractSupport.from_contract(
        provenance, interfaces=(fixture_interface(provenance).ref,)
    ).read_actions == ("read-provenance",)
    with pytest.raises(ValueError):
        ObserverContractPayload(**shared, read_actions=("read-inputs", "read-provenance"))
    descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="fixture-observer/v1",
            implementation_version="1",
            source_revision="fixture",
            image_id="sha256:" + "a" * 64,
            contracts=(
                ObserverContractSupport.from_contract(
                    provenance, interfaces=(fixture_interface(provenance).ref,)
                ),
            ),
        )
    )
    subject = WorkArtifactSubject(
        id="b" * 64,
        role="primary",
        collection=CollectionRootIdentityRef(
            collection_id="1",
            archive_root_sha256="c" * 64,
            artifact_set_identity="d" * 64,
        ),
        artifact_id="e" * 64,
        bytes="1",
        sha256="f" * 64,
    )
    assert subject.collection.to_identity().artifact_set_identity == "d" * 64
    assert CollectionRootIdentityRef.from_identity(subject.collection.to_identity()) == (
        subject.collection
    )
    correct = ContentObservationRequest.seal(
        observation_payload(
            contract=provenance,
            work_id="1" * 64,
            observer_registration_id="fixture",
            observer_descriptor_sha256=descriptor.descriptor_sha256,
            observer_contract_id=provenance.id,
            observer_contract_sha256=provenance.contract_sha256,
            read_actions=("read-provenance",),
            subjects=(subject,),
        )
    )
    assert validate_observation_request(correct, descriptor).read_actions == ("read-provenance",)
    forged = ContentObservationRequest.seal(
        observation_payload(
            contract=provenance,
            work_id=correct.work_id,
            observer_registration_id=correct.observer_registration_id,
            observer_descriptor_sha256=correct.observer_descriptor_sha256,
            observer_contract_id=correct.observer_contract_id,
            observer_contract_sha256=correct.observer_contract_sha256,
            read_actions=("read-inputs",),
            subjects=correct.subjects,
        )
    )
    with pytest.raises(ValueError, match="read authority"):
        validate_observation_request(forged, descriptor)


def test_observer_author_surface_reuses_one_model_implementation() -> None:
    assert ContentObservationRequest is shared_models.ContentObservationRequest
    assert ObserverContract is shared_models.ObserverContract
    assert _OBSERVER_AUTHOR_SYMBOLS.isdisjoint(stove0_protocol.__all__)


def test_only_the_exact_schema_only_profile_can_omit_conformance_vectors() -> None:
    altered = SemanticValidationProfile.seal(
        SemanticValidationProfilePayload(
            id=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE.id,
            rules=("fixture.different-rule/v1",),
        )
    )
    with pytest.raises(ValueError, match="conformance-vector identity"):
        ObserverContractPayload(
            id="fixture.observation/v1",
            options_schema=JsonSchemaValidationProfile.from_schema(
                "fixture.options/v1",
                {"type": "object", "additionalProperties": False},
            ),
            facts_schema=JsonSchemaValidationProfile.from_schema(
                "fixture.facts/v1",
                {"type": "object", "additionalProperties": False},
            ),
            facts_semantics=altered,
        )


def test_repository_consumers_use_the_observer_author_surface() -> None:
    root = Path(__file__).resolve().parents[5]
    violations: list[str] = []
    for base in ("packages", "some-implementations", "tests"):
        for path in root.joinpath(base).rglob("*.py"):
            implementation_models = (
                path.name == "models.py" and path.parent.name == "stove0_protocol"
            )
            if path == Path(__file__) or implementation_models:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ImportFrom) or node.module != "stove0_protocol":
                    continue
                leaked = sorted(
                    alias.name for alias in node.names if alias.name in _OBSERVER_AUTHOR_SYMBOLS
                )
                if leaked:
                    violations.append(f"{path.relative_to(root)}: {', '.join(leaked)}")
    assert not violations, violations
