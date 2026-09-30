"""Selected producer vocabulary applied to controller-sealed output declarations."""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterator, Mapping
from typing import Any

from a_riverhog_direct_relations_contract_lib import resolve_output_relations
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client.completion_records import CompletionRecords
from riverhog_protocol.collection_record_preimages import iter_canonical_record_sequence
from riverhog_provenance import assertion, evidence
from stove0_target_protocol import OutputArtifact


def completion_output_relationships(
    records: CompletionRecords, journal_id: str, agent: str
) -> Iterator[Mapping[str, Any]]:
    """Record only exact accepted output edges, without revising early primaries.

    Both inventories live in bounded disk spools. Lookup uses immutable operation
    keys and exact primary State references, never paths, hints or equal bytes.
    """

    namespace = uuid.UUID(journal_id.removeprefix("urn:uuid:"))
    previous = None
    count = 0

    def resolve(output_id: str) -> object:
        return records.output_for_key(output_id)["state"]

    for value in iter_canonical_record_sequence(
        records.record("target-output-declarations").read()
    ):
        product = OutputArtifact.model_validate(value)
        if previous is not None and product.id <= previous:
            raise ValueError("accepted output declarations are duplicated or out of order")
        previous = product.id
        count += 1
        binding = records.output_for_key(product.id)
        if (
            binding["artifact_id"] != product.artifact_id
            or int(binding["bytes"]) != product.bytes
            or binding["sha256"] != product.sha256
        ):
            raise ValueError("accepted target product differs from its exact published member")
        planned = product.model_dump(mode="json", exclude_none=True)
        planned["output_id"] = planned.pop("id")
        for subject, predicate, target in resolve_output_relations(planned, resolve):
            key = hashlib.sha256(canonical_json_bytes([subject, predicate, target])).hexdigest()
            row = assertion(
                "extension",
                agent,
                object_id="urn:uuid:" + str(uuid.uuid5(namespace, "relation:" + key)),
                assertion_id="urn:uuid:" + str(uuid.uuid5(namespace, "assertion:" + key)),
                subject=subject,
                property=predicate,
                value={"type": "reference", "value": target},
                evidence_items=[evidence(agent, "process_record")],
            )
            yield {"extensions": [row]}
    if count != records.output_count:
        raise ValueError("accepted target products do not cover every published output")


__all__ = ["completion_output_relationships"]
