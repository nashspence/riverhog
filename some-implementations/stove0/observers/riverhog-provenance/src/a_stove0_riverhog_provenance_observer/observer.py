"""Observe exact primary canonical facts or forward its Occurrence hint."""

from __future__ import annotations

import importlib.metadata
from typing import cast

from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_OBSERVER_CONTRACT,
    validate_materialization_hint_facts,
)
from a_stove0_riverhog_provenance_evidence_contract_lib import (
    CORE_PROVENANCE_OBSERVATION_ID,
    CORE_PROVENANCE_OBSERVER_CONTRACT,
    CoreProvenanceOptions,
    validate_core_provenance_facts,
)
from pydantic import JsonValue
from stove0_observer_protocol import (
    ContentObservationRequest,
    ContentObservationResult,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    canonical_json_bytes,
)
from stove0_observer_support import ContentObservationResultBuilder, ContentObservationRuntime

from .facts import extract_core_facts, extract_materialization_hint_fact


def _version() -> str:
    try:
        return importlib.metadata.version("a-stove0-riverhog-provenance-observer")
    except importlib.metadata.PackageNotFoundError:
        return "development"


class RiverhogProvenanceObserver:
    def __init__(self, *, source_revision: str = "unknown", image_id: str) -> None:
        self._descriptor = ObserverDescriptor.seal(
            ObserverDescriptorPayload(
                implementation_id="a-stove0-riverhog-provenance-observer/v1",
                implementation_version=_version(),
                source_revision=source_revision,
                image_id=image_id,
                contracts=(
                    ObserverContractSupport.from_contract(
                        MATERIALIZATION_HINT_OBSERVER_CONTRACT,
                        preferred_subject_batch_size=1,
                    ),
                    ObserverContractSupport.from_contract(
                        CORE_PROVENANCE_OBSERVER_CONTRACT,
                        preferred_subject_batch_size=1,
                    ),
                ),
            )
        )

    def descriptor(self) -> ObserverDescriptor:
        return self._descriptor

    def observe(
        self, request: ContentObservationRequest, runtime: ContentObservationRuntime
    ) -> ContentObservationResult:
        builder = ContentObservationResultBuilder(self._descriptor, request)
        try:
            core = request.observer_contract_id == CORE_PROVENANCE_OBSERVATION_ID
            options = (
                CoreProvenanceOptions.model_validate_json(canonical_json_bytes(request.options))
                if core
                else None
            )
            facts = []
            for subject in request.subjects:
                runtime.heartbeat()
                claimed = runtime.open_provenance(subject)
                summary = claimed.bound_summary()
                if core and options is not None:
                    facts.append(
                        extract_core_facts(
                            subject,
                            claimed.binding,
                            summary,
                            predicates=options.predicates,
                            resolve_external=claimed.resolve_external_reference,
                        )
                    )
                else:
                    facts.append(
                        extract_materialization_hint_fact(subject, claimed.binding, summary)
                    )
            document = {"artifacts": facts}
            validated = (
                validate_core_provenance_facts(document, request.subjects, request.options)
                if core
                else validate_materialization_hint_facts(document, request.subjects)
            )
            return builder.observed(cast(dict[str, JsonValue], validated.model_dump(mode="json")))
        except (ValueError, RuntimeError, OSError) as exc:
            return builder.failed(
                code="canonical-occurrence-unavailable",
                message="The exact delivered Occurrence could not be validated.",
                retryable=isinstance(exc, (RuntimeError, OSError)),
            )


__all__ = ["RiverhogProvenanceObserver"]
