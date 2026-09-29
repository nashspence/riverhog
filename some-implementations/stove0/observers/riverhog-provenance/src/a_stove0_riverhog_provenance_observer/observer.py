"""Read a selected member's exact primary canonical Occurrence."""

from __future__ import annotations

import importlib.metadata
from typing import cast

from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_OBSERVER_CONTRACT,
    validate_materialization_hint_facts,
)
from pydantic import JsonValue
from stove0_observer_protocol import (
    ContentObservationRequest,
    ContentObservationResult,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
)
from stove0_observer_support import ContentObservationResultBuilder, ContentObservationRuntime

from .facts import extract_materialization_hint_fact


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
            facts = []
            for subject in request.subjects:
                runtime.heartbeat()
                claimed = runtime.open_provenance(subject)
                facts.append(
                    extract_materialization_hint_fact(
                        subject, claimed.binding, claimed.bound_summary()
                    )
                )
            document = {"artifacts": facts}
            validated = validate_materialization_hint_facts(document, request.subjects)
            return builder.observed(cast(dict[str, JsonValue], validated.model_dump(mode="json")))
        except (ValueError, RuntimeError, OSError) as exc:
            return builder.failed(
                code="canonical-occurrence-unavailable",
                message="The exact delivered Occurrence could not be validated.",
                retryable=isinstance(exc, (RuntimeError, OSError)),
            )


__all__ = ["RiverhogProvenanceObserver"]
