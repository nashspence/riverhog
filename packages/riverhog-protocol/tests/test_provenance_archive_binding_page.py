from __future__ import annotations

import pytest
from riverhog_protocol import (
    CollectionArtifactProvenanceBindingBatchDocument,
    validate_archive_binding_page,
)


def _bindings(count: int) -> list[dict[str, object]]:
    return [
        {
            "artifact_id": f"{ordinal:064x}",
            "journal": {
                "journal_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
                "through": {
                    "entry_id": "urn:uuid:22222222-2222-4222-8222-222222222222",
                    "sequence": "0",
                    "json_sha256": "a" * 64,
                },
                "prefix_sha256": "b" * 64,
                "prefix_bytes": "1",
            },
            "delivery_association_id": "urn:uuid:33333333-3333-4333-8333-333333333333",
        }
        for ordinal in range(count)
    ]


def test_archive_pages_are_not_capped_by_http_registration_batch() -> None:
    rows = _bindings(512)
    with pytest.raises(ValueError):
        CollectionArtifactProvenanceBindingBatchDocument.model_validate({"bindings": rows})
    assert len(validate_archive_binding_page(rows, max_members=512)) == 512
    with pytest.raises(ValueError, match="member count"):
        validate_archive_binding_page(_bindings(513), max_members=512)


def test_archive_page_rejects_duplicate_and_out_of_order_members() -> None:
    rows = _bindings(2)
    with pytest.raises(ValueError, match="ordered"):
        validate_archive_binding_page(rows[::-1], max_members=512)
    with pytest.raises(ValueError, match="ordered"):
        validate_archive_binding_page([rows[0], rows[0]], max_members=512)
