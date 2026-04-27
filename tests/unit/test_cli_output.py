from __future__ import annotations

from arc_cli.output import format_fetch, format_pin


def test_format_pin_includes_fetch_guidance() -> None:
    rendered = format_pin(
        {
            "target": "docs/tax/2022/invoice-123.pdf",
            "pin": True,
            "hot": {"state": "waiting", "present_bytes": 0, "missing_bytes": 19},
            "fetch": {
                "id": "fx-1",
                "state": "waiting_media",
                "copies": [
                    {
                        "id": "20260420T040001Z-1",
                        "volume_id": "20260420T040001Z",
                        "location": "vault-a/shelf-01",
                    }
                ],
            },
        }
    )

    assert "docs/tax/2022/invoice-123.pdf" in rendered
    assert "fx-1" in rendered
    assert "20260420T040001Z-1" in rendered
    assert "candidate copies" in rendered


def test_format_fetch_lists_pending_partial_and_expiry() -> None:
    rendered = format_fetch(
        {
            "id": "fx-1",
            "target": "docs/tax/2022/invoice-123.pdf",
            "state": "waiting_media",
        },
        {
            "id": "fx-1",
            "target": "docs/tax/2022/invoice-123.pdf",
            "entries": [
                {
                    "id": "e1",
                    "path": "tax/2022/invoice-123.pdf",
                    "bytes": 19,
                },
                {
                    "id": "e2",
                    "path": "tax/2022/receipt-456.pdf",
                    "bytes": 10,
                    "uploaded_bytes": 3,
                    "upload_state": "partial",
                    "upload_state_expires_at": "2026-04-23T00:00:00Z",
                },
                {
                    "id": "e3",
                    "path": "tax/2022/check-789.pdf",
                    "bytes": 12,
                    "recovery_bytes": 12,
                    "uploaded_bytes": 12,
                    "upload_state": "byte_complete",
                    "upload_state_expires_at": None,
                },
            ],
        },
    )

    assert "fx-1" in rendered
    assert "pending" in rendered
    assert "tax/2022/invoice-123.pdf" in rendered
    assert "partial" in rendered
    assert "tax/2022/receipt-456.pdf" in rendered
    assert "expires 2026-04-23T00:00:00Z" in rendered
    assert "byte-complete" in rendered
    assert "tax/2022/check-789.pdf" in rendered
