from __future__ import annotations

import hashlib
import json

import pytest
import rfc8785
from riverhog_canonical_json import (
    CanonicalJsonError,
    canonical_json_bytes,
    canonical_json_sha256,
    format_scalar,
    parse_identity_json,
    parse_scalar,
    require_canonical_json,
    scalar_schema,
)


def test_jcs_orders_utf16_keys_and_preserves_unicode_code_points() -> None:
    value = {"\ue000": "e\u0301", "😀": "\u00e9", "a": True}
    encoded = canonical_json_bytes(value)
    assert encoded == '{"a":true,"😀":"é","\ue000":"e\u0301"}'.encode()
    assert canonical_json_sha256(value) == hashlib.sha256(encoded).hexdigest()
    assert require_canonical_json(encoded) == value


def test_complete_ascii_strings_preserve_control_escaping_and_exact_identity() -> None:
    text = "".join(chr(code) for code in range(128)) * 128
    value = {text: text}
    expected = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    assert canonical_json_bytes(value) == expected
    assert require_canonical_json(expected) == value
    assert canonical_json_sha256(value) == hashlib.sha256(expected).hexdigest()


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        False,
        0,
        -(2**53 - 1),
        2**53 - 1,
        "é 😀 e\u0301 \u2028\u2029",
        {"z": [None, True, False, -17], "a": {'\\"\n\x00': "é 😀"}},
        {"".join(chr(code) for code in range(128)): "".join(chr(code) for code in range(128))},
        {chr(code): chr(code) for code in reversed(range(128))},
        {"safe": 1.0, "negative_zero": -0.0, "tiny": 1e-27, "large": 1e30},
        {"a": [{"\ue000": "BMP", "😀": "astral"}]},
        {
            "ready": [{"artifact_id": "opaque", "size": 17, "hint": "é 😀"}],
            "facts": {"duration": 0.1, "streams": [{"\ue000": 2, "😀": 1}]},
        },
    ],
)
def test_canonical_encoding_matches_rfc8785_for_nested_primitives_and_utf16_keys(
    value: object,
) -> None:
    expected = rfc8785.dumps(value)  # type: ignore[arg-type]
    assert canonical_json_bytes(value) == expected
    assert canonical_json_sha256(value) == hashlib.sha256(expected).hexdigest()


def test_exact_large_integer_keeps_jcs_numeric_spelling() -> None:
    value = {"exact": 2**53}
    expected = rfc8785.dumps({"exact": float(2**53)})
    assert canonical_json_bytes(value) == expected


def test_exact_integer_rejects_rounded_jcs_spelling() -> None:
    with pytest.raises(CanonicalJsonError, match="JCS spelling changes"):
        canonical_json_bytes({"exact": 2**60})


@pytest.mark.parametrize(
    "codepoint",
    [0xD800, 0xDFFF, 0xFDD0, 0xFDEF]
    + [(plane << 16) | suffix for plane in range(17) for suffix in (0xFFFE, 0xFFFF)],
)
def test_identity_rejects_noncharacters_and_surrogates_mixed_with_ascii(codepoint: int) -> None:
    invalid = "ASCII prefix " + chr(codepoint) + " ASCII suffix"
    for value in ({"value": invalid}, {invalid: "value"}):
        with pytest.raises(CanonicalJsonError) as exc_info:
            canonical_json_bytes(value)
        assert exc_info.value.reason == "unicode"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (b' { "b": 2, "a": 1 } ', b'{"a":1,"b":2}'),
        (b"[-0,-0.0,0.0]", b"[0,0,0]"),
        (b"[1,1.0,1e0,100e-2]", b"[1,1,1,1]"),
        (b"9007199254740992", b"9007199254740992"),
        (b'{"measurement":0.1}', b'{"measurement":0.1}'),
    ],
)
def test_admitted_json_has_one_jcs_encoding(raw: bytes, expected: bytes) -> None:
    assert canonical_json_bytes(parse_identity_json(raw)) == expected


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (b'{"x":1,"\\u0078":2}', "duplicate"),
        (b'"\\ud800"', "unicode"),
        (b'"\\ufdd0"', "unicode"),
        (b"9007199254740993", "numeric"),
        (b"9223372036854775807", "numeric"),
        (b"1e400", "numeric"),
        (b"NaN", "numeric"),
        (b"01", "syntax"),
    ],
)
def test_identity_admission_rejects_loss_or_ambiguity(raw: bytes, reason: str) -> None:
    with pytest.raises(CanonicalJsonError) as exc_info:
        parse_identity_json(raw)
    assert exc_info.value.reason == reason


def test_binary64_domain_rejects_underflow() -> None:
    assert parse_identity_json(b'{"measurement":0.1}') == {"measurement": 0.1}
    with pytest.raises(CanonicalJsonError, match="underflows"):
        parse_identity_json(b"1e-400")


def test_canonical_text_is_verified_after_strict_admission() -> None:
    with pytest.raises(CanonicalJsonError) as exc_info:
        require_canonical_json(b'{"b":2,"a":1}')
    assert exc_info.value.reason == "canonical"
    with pytest.raises(CanonicalJsonError) as exc_info:
        require_canonical_json(b'{"a":1,"a":2}')
    assert exc_info.value.reason == "duplicate"


def test_named_scalar_codecs_preserve_full_semantic_capacity() -> None:
    for domain, value in (
        ("sequence63", (1 << 63) - 1),
        ("sequence256", (1 << 256) - 1),
        ("nonnegative", 10**5000 + 17),
    ):
        text = format_scalar(domain, value)
        assert parse_scalar(domain, text) == value
        assert scalar_schema(domain)["type"] == "string"
    assert format_scalar("sequence256", 1) == "0" * 63 + "1"


@pytest.mark.parametrize(
    ("domain", "value"),
    [
        ("sequence63", "9223372036854775808"),
        ("sequence63", "01"),
        ("sequence256", "F" * 64),
        ("sequence256", "0" * 63),
        ("nonnegative", "1\n"),
        ("nonnegative", 1),
    ],
)
def test_scalar_decoders_reject_noncanonical_values(domain: str, value: object) -> None:
    with pytest.raises(CanonicalJsonError):
        parse_scalar(domain, value)  # type: ignore[arg-type]
