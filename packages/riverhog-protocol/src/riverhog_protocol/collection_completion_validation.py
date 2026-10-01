"""Relationships between accepted completion declarations and exact archived preimages."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from itertools import zip_longest
from typing import Any

from riverhog_archive_contracts import read_bounded_history_object
from riverhog_canonical_json import canonical_json_bytes, canonical_json_sha256

from .collection_completion import CollectionCompletionRequirementDocument
from .collection_record_preimages import CollectionRecordPreimages, iter_canonical_record_sequence
from .collection_workflow_transport import (
    ArtifactDispositionOutputPageDocument,
    ArtifactDispositionPageDocument,
)
from .collection_workflows import (
    ArtifactDisposition,
    ArtifactDispositionOutput,
    ArtifactDispositionSetIdentity,
)


def validate_disposition_record_pages(
    chunks: Iterable[bytes], *, identity: ArtifactDispositionSetIdentity
) -> None:
    """Verify complete accepted pages without deciding live settlement or retirement."""

    digests = {kind: hashlib.sha256() for kind in ("dispositions", "outputs")}
    counts = {kind: 0 for kind in digests}
    terminal = {kind: False for kind in digests}
    previous: dict[str, tuple[Any, ...] | None] = {kind: None for kind in digests}
    output_artifacts = 0
    prior_output_id: str | None = None
    for record in iter_canonical_record_sequence(chunks):
        if set(record) != {"kind", "page"} or record["kind"] not in digests:
            raise ValueError("completion disposition page kind or fields are invalid")
        kind = record["kind"]
        if terminal[kind] or (kind == "outputs" and not terminal["dispositions"]):
            raise ValueError("completion disposition pages are repeated or out of order")
        page: ArtifactDispositionPageDocument | ArtifactDispositionOutputPageDocument
        rows: Iterable[ArtifactDisposition | ArtifactDispositionOutput]
        if kind == "dispositions":
            page = ArtifactDispositionPageDocument.model_validate(record["page"])
            rows = (
                ArtifactDisposition.from_mapping(row.model_dump(mode="json", exclude_none=True))
                for row in page.dispositions
            )
            expected_count = identity.disposition_count
        else:
            page = ArtifactDispositionOutputPageDocument.model_validate(record["page"])
            rows = (
                ArtifactDispositionOutput.from_mapping(
                    row.model_dump(mode="json", exclude_none=True)
                )
                for row in page.outputs
            )
            expected_count = identity.output_edge_count
        if (
            page.identity.model_dump(mode="json") != identity.as_dict()
            or page.start_ordinal != counts[kind]
        ):
            raise ValueError(
                "completion disposition page differs from its accepted identity/extent"
            )
        for row in rows:
            key: tuple[Any, ...]
            if isinstance(row, ArtifactDisposition):
                key = (row.input_collection_id, str(row.input_artifact_id))
            else:
                key = (
                    str(row.output_artifact_id),
                    row.input_collection_id,
                    str(row.input_artifact_id),
                )
                if str(row.output_artifact_id) != prior_output_id:
                    output_artifacts += 1
                    prior_output_id = str(row.output_artifact_id)
            previous_key = previous[kind]
            if previous_key is not None and key <= previous_key:
                raise ValueError("completion disposition rows are duplicate or unordered")
            previous[kind] = key
            counts[kind] += 1
            digests[kind].update(canonical_json_bytes(row.as_dict()) + b"\n")
        if page.next_ordinal is None:
            if counts[kind] != expected_count:
                raise ValueError("completion disposition terminal omits accepted rows")
            terminal[kind] = True
        elif page.next_ordinal != counts[kind] or counts[kind] >= expected_count:
            raise ValueError("completion disposition continuation extent differs")
    if not all(terminal.values()) or output_artifacts != identity.output_artifact_count:
        raise ValueError("completion disposition pages are incomplete")
    actual = canonical_json_sha256(
        {
            "format": "riverhog-artifact-disposition-set/v1",
            "disposition_count": str(counts["dispositions"]),
            "dispositions_sha256": digests["dispositions"].hexdigest(),
            "output_edge_count": str(counts["outputs"]),
            "output_artifact_count": str(output_artifacts),
            "outputs_sha256": digests["outputs"].hexdigest(),
        }
    )
    if actual != identity.sha256:
        raise ValueError("completion disposition pages differ from the sealed commitment")


def validate_completion_preimages(
    *,
    requirement: CollectionCompletionRequirementDocument,
    completion: Mapping[str, Any],
    extensions: Iterable[Mapping[str, Any]],
    subject: Mapping[str, Any],
    expected_construction: Mapping[str, Any],
    expected_outputs: Iterable[Mapping[str, Any]],
    expected_imports: Iterable[Mapping[str, Any]],
    disposition: ArtifactDispositionSetIdentity,
    expected_records_sha256: str | None = None,
) -> dict[str, dict[str, str]]:
    if (
        completion["requirement_sha256"] != requirement.identity
        or completion["execution_id"] != requirement.execution_id
        or completion["disposition_set_sha256"] != disposition.sha256
    ):
        raise ValueError("completion record differs from its accepted construction")
    with CollectionRecordPreimages(extensions, subject=subject) as retained:
        retained.validate(expected_kinds=requirement.record_kinds)
        if (
            expected_records_sha256 is not None
            and retained.inventory_sha256() != expected_records_sha256
        ):
            raise ValueError("completion preimages differ from the accepted recording inventory")
        for kind, expected in (
            ("controller", requirement.controller_evidence_sha256),
            ("target-execution", completion["execution_sha256"]),
            ("output-bindings", completion["output_bindings_sha256"]),
            ("input-history-bindings", completion["input_history_bindings_sha256"]),
        ):
            if retained.identity(kind)[0] != expected:
                raise ValueError("completion record identity differs from its exact manifest")
        for kind, expected in (
            ("accepted-construction", expected_construction),
            ("disposition-identity", disposition.as_dict()),
        ):
            actual = read_bounded_history_object(retained.chunks(kind), 64 * 1024)
            if actual != canonical_json_bytes(dict(expected)):
                raise ValueError("completion required record differs from accepted authority")
        for kind, expected_rows in (
            ("output-bindings", expected_outputs),
            ("input-history-bindings", expected_imports),
        ):
            sentinel = object()
            for actual_row, expected_row in zip_longest(
                iter_canonical_record_sequence(retained.chunks(kind)),
                expected_rows,
                fillvalue=sentinel,
            ):
                if actual_row != expected_row:
                    raise ValueError(
                        "completion output/State/history correspondence "
                        "is incomplete or substituted"
                    )
        validate_disposition_record_pages(
            retained.chunks("disposition-pages"), identity=disposition
        )

        return {
            kind: {"sha256": retained.identity(kind)[0], "bytes": str(retained.identity(kind)[1])}
            for kind in requirement.record_kinds
        }


__all__ = ["validate_completion_preimages", "validate_disposition_record_pages"]
