"""Assumed publication projections for transaction-only workflow fixtures.

These rows do not qualify journal bytes or archive validation. The HTTP lifecycle
witness constructs the real completion journal and encrypted archive.
"""

from riverhog_protocol.collection_completion import (
    COMPLETION_REQUIRED_RECORD_KINDS,
    CollectionCompletionPublicationReceiptDocument,
    CollectionCompletionRequirementDocument,
)
from riverhog_protocol.collection_record_preimages import completion_record_inventory_sha256
from riverhog_protocol.collection_workflows import (
    CollectionDerivation,
    canonical_json_bytes,
    canonical_json_sha256,
)


def published_completion_projection(derivation: CollectionDerivation) -> str:
    requirement = CollectionCompletionRequirementDocument(
        execution_id=derivation.execution_id,
        execution_envelope_sha256=derivation.execution_envelope_sha256,
        controller_evidence_sha256=derivation.controller_evidence_sha256,
        record_kinds=COMPLETION_REQUIRED_RECORD_KINDS,
    )
    records = {kind: {"sha256": "0" * 64, "bytes": "1"} for kind in requirement.record_kinds}
    records["target-execution"]["sha256"] = derivation.execution_sha256
    records["controller"]["sha256"] = derivation.controller_evidence_sha256
    records["disposition-identity"]["sha256"] = canonical_json_sha256(
        derivation.disposition_set.as_dict()
    )
    receipt = CollectionCompletionPublicationReceiptDocument.model_validate(
        {
            "requirement_sha256": requirement.identity,
            "records_sha256": completion_record_inventory_sha256(
                (kind, value["sha256"], int(value["bytes"]))
                for kind, value in sorted(records.items())
            ),
            "journal": {
                "journal_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
                "through": {
                    "entry_id": "urn:uuid:22222222-2222-4222-8222-222222222222",
                    "sequence": "0",
                    "json_sha256": "0" * 64,
                },
                "prefix_sha256": "0" * 64,
                "prefix_bytes": "1",
            },
            "records": records,
        }
    )
    return canonical_json_bytes(receipt.model_dump(mode="json")).decode("utf-8")
