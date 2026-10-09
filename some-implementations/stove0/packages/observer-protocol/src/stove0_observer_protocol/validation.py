"""Executable acceptance for the public Stove0 observer contract."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import JsonValue
from stove0_protocol.models import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    SHA256_PATTERN,
    AcceptedObservationJob,
    ContentObservationRequest,
    ContentObservationResult,
    ObservationJobStatus,
    ObserverContractSupport,
    ObserverDescriptor,
    SemanticValidationProfile,
    canonical_json_bytes,
)
from stove0_protocol.observation_interfaces import ExactDocumentRef
from stove0_protocol.observation_views import CoverageStatus, StatusResolver

FactsSemanticValidator = Callable[[ContentObservationRequest, Mapping[str, object]], None]
FactsStatusResolver = Callable[
    [tuple[str, ...], dict[str, JsonValue]], Mapping[str, CoverageStatus]
]


@runtime_checkable
class SemanticStatusProvider(Protocol):
    def resolve_status(
        self, profile_id: str, profile_sha256: str
    ) -> FactsStatusResolver | None: ...


def semantic_status_resolver(provider: SemanticValidatorProvider | None) -> StatusResolver:
    def resolve(
        profile: ExactDocumentRef, subjects: tuple[str, ...], facts: dict[str, JsonValue]
    ) -> Mapping[str, CoverageStatus]:
        resolver = (
            provider.resolve_status(profile.id, profile.sha256)
            if isinstance(provider, SemanticStatusProvider)
            else None
        )
        if resolver is None:
            raise ValueError("exact semantic completeness extraction is unavailable")
        return resolver(subjects, facts)

    return resolve


@dataclass(frozen=True, slots=True)
class SemanticValidatorBinding:
    """One contract-owned validator bound to an exact portable profile identity."""

    profile_id: str
    profile_sha256: str
    validator: FactsSemanticValidator
    status_resolver: FactsStatusResolver | None = None

    @classmethod
    def from_profile(
        cls,
        profile: SemanticValidationProfile,
        validator: FactsSemanticValidator,
        *,
        status_resolver: FactsStatusResolver | None = None,
    ) -> SemanticValidatorBinding:
        return cls(
            profile_id=profile.id,
            profile_sha256=profile.profile_sha256,
            validator=validator,
            status_resolver=status_resolver,
        )

    def __post_init__(self) -> None:
        if not self.profile_id.strip():
            raise ValueError("semantic validator profile ID must be nonempty")
        if re.fullmatch(SHA256_PATTERN, self.profile_sha256) is None:
            raise ValueError("semantic validator profile digest must be SHA-256")
        if not callable(self.validator):
            raise TypeError("semantic validator must be callable")
        if self.status_resolver is not None and not callable(self.status_resolver):
            raise TypeError("semantic status resolver must be callable")


class SemanticValidatorProvider(Protocol):
    """Resolve enabled contract semantics without importing them into generic consumers."""

    def resolve(
        self,
        profile_id: str,
        profile_sha256: str,
    ) -> FactsSemanticValidator | None: ...


class SemanticValidatorRegistry:
    """Immutable exact-profile registry assembled by application composition."""

    def __init__(self, bindings: Iterable[SemanticValidatorBinding] = ()) -> None:
        materialized = tuple(bindings)
        validators: dict[tuple[str, str], FactsSemanticValidator] = {}
        for binding in materialized:
            key = (binding.profile_id, binding.profile_sha256)
            if key in validators:
                raise ValueError("semantic validator profile is registered more than once")
            validators[key] = binding.validator
        self._validators = validators
        self._statuses = {
            (binding.profile_id, binding.profile_sha256): binding.status_resolver
            for binding in materialized
            if binding.status_resolver is not None
        }

    def resolve(
        self,
        profile_id: str,
        profile_sha256: str,
    ) -> FactsSemanticValidator | None:
        return self._validators.get((profile_id, profile_sha256))

    def resolve_status(self, profile_id: str, profile_sha256: str) -> FactsStatusResolver | None:
        return self._statuses.get((profile_id, profile_sha256))


def validate_observation_request(
    request: ContentObservationRequest,
    descriptor: ObserverDescriptor,
) -> ObserverContractSupport:
    """Validate one sealed request against the exact advertised contract."""

    if descriptor.descriptor_sha256 != request.observer_descriptor_sha256:
        raise ValueError("observer descriptor differs from the sealed request")
    support = descriptor.support_for(request.observer_contract_id)
    if support.contract_sha256 != request.observer_contract_sha256:
        raise ValueError("observer contract differs from the sealed request")
    if request.interface not in support.interfaces:
        raise ValueError("observer does not advertise the exact selected observation interface")
    if request.read_actions != support.read_actions:
        raise ValueError("observer read authority differs from the advertised contract")
    if support.maximum_result_bytes is not None and (
        request.maximum_result_bytes is None
        or request.maximum_result_bytes > support.maximum_result_bytes
    ):
        raise ValueError("observation request exceeds the observer contract result limit")
    try:
        Draft202012Validator(support.options_schema.document).validate(request.options)
    except JsonSchemaValidationError as exc:
        raise ValueError("observation request options violate their advertised schema") from exc
    return support


def require_semantic_validators(
    provider: SemanticValidatorProvider | None,
    descriptor: ObserverDescriptor,
) -> None:
    """Fail closed unless every advertised non-schema profile can be accepted locally."""

    for support in descriptor.contracts:
        profile = support.facts_semantics
        if profile == JSON_SCHEMA_ONLY_SEMANTIC_PROFILE:
            continue
        if provider is None or provider.resolve(profile.id, profile.profile_sha256) is None:
            raise ValueError(
                "semantic validator is unavailable for advertised observer profile "
                f"{profile.id}@{profile.profile_sha256}"
            )


def accept_observation_result(
    result: ContentObservationResult,
    request: ContentObservationRequest,
    descriptor: ObserverDescriptor,
    semantic_validators: SemanticValidatorProvider | None = None,
) -> None:
    """Apply the complete structural and semantic observer-result acceptance domain."""

    support = validate_observation_result_structure(result, request, descriptor)
    if result.state != "observed":
        return
    assert result.facts is not None
    profile = support.facts_semantics
    if profile == JSON_SCHEMA_ONLY_SEMANTIC_PROFILE:
        return
    validator = (
        semantic_validators.resolve(profile.id, profile.profile_sha256)
        if semantic_validators is not None
        else None
    )
    if validator is None:
        raise ValueError(
            "semantic validator is unavailable for observer profile "
            f"{profile.id}@{profile.profile_sha256}"
        )
    validator(request, result.facts)


def validate_observation_status(
    status: ObservationJobStatus,
    accepted: AcceptedObservationJob,
    descriptor: ObserverDescriptor,
    semantic_validators: SemanticValidatorProvider | None = None,
) -> None:
    if status.job_id != accepted.job_id or status.request_id != accepted.request.request_id:
        raise ValueError("observer status belongs to another invocation")
    validate_observation_request(accepted.request, descriptor)
    if status.result is not None:
        accept_observation_result(status.result, accepted.request, descriptor, semantic_validators)


def validate_observation_result_structure(
    result: ContentObservationResult,
    request: ContentObservationRequest,
    descriptor: ObserverDescriptor,
) -> ObserverContractSupport:
    """Apply the extension-agnostic structural and schema acceptance domain."""

    support = validate_observation_request(request, descriptor)
    if result.request_id != request.request_id:
        raise ValueError("observation result does not bind the request")
    if (
        result.observer_contract_id != support.contract_id
        or result.observer_contract_sha256 != support.contract_sha256
        or result.observer.descriptor_sha256 != request.observer_descriptor_sha256
    ):
        raise ValueError("observation result does not bind the accepted observer contract")
    if result.subjects != request.subjects:
        raise ValueError("observation result subjects differ from the request")
    if result.state == "observed":
        if result.facts_schema != support.facts_schema or result.facts is None:
            raise ValueError("observation result uses an unexpected facts schema")
        try:
            Draft202012Validator(support.facts_schema.document).validate(result.facts)
        except JsonSchemaValidationError as exc:
            raise ValueError("observation facts violate their advertised schema") from exc
    if request.maximum_result_bytes is not None:
        encoded = canonical_json_bytes(result.model_dump(mode="json", exclude_none=True))
        if len(encoded) > request.maximum_result_bytes:
            raise ValueError("observation result exceeds the requested result-size limit")
    return support


__all__ = [
    "FactsSemanticValidator",
    "SemanticValidatorBinding",
    "SemanticValidatorProvider",
    "SemanticValidatorRegistry",
    "accept_observation_result",
    "require_semantic_validators",
    "validate_observation_request",
    "validate_observation_result_structure",
]
