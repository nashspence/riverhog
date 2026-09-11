from __future__ import annotations

import pytest
from http_api_contracts import (
    HttpOperationErrorAuthority,
    parse_operation_error_payload,
)


def _authority() -> HttpOperationErrorAuthority:
    return HttpOperationErrorAuthority.from_codes(
        common=("bad_request", "internal_error"),
        operation={"get_item": ("not_found",)},
        exact={"health_ready": ("service_unavailable",)},
    )


def test_operation_error_authority_exposes_exact_code_status_pairs() -> None:
    authority = _authority()

    assert [(item.code, item.status) for item in authority.errors_for("get_item")] == [
        ("bad_request", 400),
        ("not_found", 404),
        ("internal_error", 500),
    ]
    assert [(item.code, item.status) for item in authority.errors_for("health_ready")] == [
        ("service_unavailable", 503)
    ]


def test_operation_error_parser_accepts_only_the_declared_pair() -> None:
    authority = _authority()

    assert parse_operation_error_payload(
        authority,
        "get_item",
        status=404,
        payload={"error": {"code": "not_found", "message": "missing"}},
    ) == ("not_found", "missing", {})

    with pytest.raises(ValueError, match="undeclared operation error"):
        parse_operation_error_payload(
            authority,
            "get_item",
            status=409,
            payload={"error": {"code": "not_found", "message": "wrong status"}},
        )


def test_operation_error_authority_rejects_ambiguous_membership() -> None:
    with pytest.raises(ValueError, match="repeat a common code"):
        HttpOperationErrorAuthority.from_codes(
            common=("bad_request",),
            operation={"get_item": ("bad_request",)},
        )
