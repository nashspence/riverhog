from __future__ import annotations

from arc_cli.output import format_images


def test_format_images_surfaces_glacier_failure_context() -> None:
    rendered = format_images(
        {
            "page": 1,
            "pages": 1,
            "per_page": 25,
            "total": 1,
            "sort": "finalized_at",
            "order": "desc",
            "images": [
                {
                    "id": "20260420T040001Z",
                    "filename": "20260420T040001Z.iso",
                    "finalized_at": "2026-04-20T04:00:01Z",
                    "collections": 1,
                    "collection_ids": ["docs"],
                    "protection_state": "partially_protected",
                    "physical_copies_registered": 1,
                    "physical_copies_required": 2,
                    "glacier": {
                        "state": "failed",
                        "object_path": None,
                        "failure": "s3 timeout",
                    },
                }
            ],
        }
    )
    assert "glacier=failed" in rendered
    assert "glacier_failure: s3 timeout" in rendered
