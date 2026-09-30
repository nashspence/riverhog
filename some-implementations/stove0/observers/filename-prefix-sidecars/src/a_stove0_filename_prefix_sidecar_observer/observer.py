"""Compare exact filenames supplied by accepted canonical locator evidence."""

from __future__ import annotations

import importlib.metadata
from typing import cast

from a_stove0_filename_prefix_sidecar_evidence_contract_lib import (
    FILENAME_OBSERVER_CONTRACT,
    FilenameQuestion,
    validate_filename_facts,
)
from a_stove0_riverhog_provenance_evidence_contract_lib import (
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

from .pairing import LocatorEvidence, compare_filenames


def _version() -> str:
    try:
        return importlib.metadata.version("a-stove0-filename-prefix-sidecar-observer")
    except importlib.metadata.PackageNotFoundError:
        return "development"


class FilenamePrefixSidecarObserver:
    def __init__(self, *, source_revision: str = "unknown", image_id: str) -> None:
        self._descriptor = ObserverDescriptor.seal(
            ObserverDescriptorPayload(
                implementation_id="a-stove0-filename-prefix-sidecar-observer/v1",
                implementation_version=_version(),
                source_revision=source_revision,
                image_id=image_id,
                contracts=(
                    ObserverContractSupport.from_contract(
                        FILENAME_OBSERVER_CONTRACT, preferred_subject_batch_size=2
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
            question = FilenameQuestion.model_validate_json(canonical_json_bytes(request.options))
            locators: dict[str, tuple[LocatorEvidence, ...]] = {}
            predecessor_ids: list[dict[str, str]] = []
            for slot in question.provenance_slots:
                predecessor = runtime.open_evidence(slot)
                CoreProvenanceOptions.model_validate_json(
                    canonical_json_bytes(predecessor.request.options)
                )
                if (
                    predecessor.request.observer_contract_id != CORE_PROVENANCE_OBSERVER_CONTRACT.id
                    or predecessor.request.observer_contract_sha256
                    != CORE_PROVENANCE_OBSERVER_CONTRACT.contract_sha256
                    or predecessor.request.read_actions != ("read-provenance",)
                    or predecessor.result.facts_schema
                    != CORE_PROVENANCE_OBSERVER_CONTRACT.facts_schema
                    or predecessor.result.facts is None
                ):
                    raise ValueError("filename question requires accepted core locator facts")
                facts = validate_core_provenance_facts(
                    predecessor.result.facts,
                    predecessor.request.subjects,
                    predecessor.request.options,
                )
                predecessor_ids.append(
                    {
                        "request_id": predecessor.request.request_id,
                        "result_sha256": predecessor.result.result_sha256,
                    }
                )
                for row in facts.artifacts:
                    if row.subject_id in locators:
                        raise ValueError("core locator evidence repeats a selected subject")
                    locators[row.subject_id] = tuple(
                        LocatorEvidence(
                            subject_id=row.subject_id,
                            locator=locator.locator,
                            context_endpoint=locator.context_endpoint.model_dump(mode="json"),
                            context_identifiers=locator.context_identifiers,
                            context_support=locator.context_support.model_dump(mode="json"),
                            locator_support=locator.locator_support.model_dump(mode="json"),
                        )
                        for locator in row.locators
                    )
            if set(locators) != {subject.id for subject in request.subjects}:
                raise ValueError("core locator evidence does not cover the selected subjects")
            statuses, candidates = compare_filenames(
                locators,
                primary_ids=question.primary_ids,
                sidecar_ids=question.sidecar_ids,
                sidecar_suffixes=question.sidecar_suffixes,
            )
            document = {
                "provenance_results": sorted(predecessor_ids, key=lambda item: item["request_id"]),
                "statuses": [
                    {
                        "subject_id": row.subject_id,
                        "status": row.status,
                        "support": [dict(item) for item in row.support],
                    }
                    for row in statuses
                ],
                "candidates": [
                    {
                        "primary_id": row.primary_id,
                        "sidecar_id": row.sidecar_id,
                        "rule": row.rule,
                        "support": [dict(item) for item in row.support],
                    }
                    for row in candidates
                ],
            }
            accepted = validate_filename_facts(
                document, request.subjects, request.options, request=request
            )
            return builder.observed(cast(dict[str, JsonValue], accepted.model_dump(mode="json")))
        except (KeyError, TypeError, ValueError, RuntimeError, OSError) as exc:
            return builder.failed(
                code="filename-evidence-unavailable",
                message="The selected canonical locator question could not be completed.",
                retryable=isinstance(exc, (RuntimeError, OSError)),
            )


__all__ = ["FilenamePrefixSidecarObserver"]
