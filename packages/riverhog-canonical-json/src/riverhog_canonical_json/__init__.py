"""RFC 8785 JSON values and named lossless scalar encodings."""

from __future__ import annotations

import hashlib
import json
import math
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, cast

import rfc8785

SAFE_INTEGER = (1 << 53) - 1
SEQUENCE63_MAX = (1 << 63) - 1
SEQUENCE256_MAX = (1 << 256) - 1

type ScalarDomain = Literal["sequence63", "sequence256", "nonnegative"]
type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]


class CanonicalJsonError(ValueError):
    """A JSON value or text cannot enter an exact canonical identity domain."""

    def __init__(self, reason: str, detail: str) -> None:
        self.reason = reason
        super().__init__(f"{reason}: {detail}")


def _string(value: str) -> str:
    for character in value:
        code = ord(character)
        if 0xD800 <= code <= 0xDFFF or 0xFDD0 <= code <= 0xFDEF or code & 0xFFFF >= 0xFFFE:
            raise CanonicalJsonError("unicode", "I-JSON excludes surrogates and noncharacters")
    return value


def _exact_integer(value: int) -> int | float:
    if -SAFE_INTEGER <= value <= SAFE_INTEGER:
        return value
    try:
        number = float(value)
    except OverflowError as exc:
        raise CanonicalJsonError("numeric", "exact integer needs a string codec") from exc
    if not math.isfinite(number) or int(number) != value:
        raise CanonicalJsonError("numeric", "integer is not exact in binary64")
    # An exact binary64 integer can still acquire a rounded decimal JCS spelling.
    if Decimal(rfc8785.dumps(number).decode("ascii")) != value:
        raise CanonicalJsonError("numeric", "JCS spelling changes the exact integer")
    return number


def _prepare(value: object) -> object:
    if value is None or type(value) is bool:
        return value
    if type(value) is str:
        return _string(value)
    if type(value) is int:
        return _exact_integer(value)
    if type(value) is float:
        if not math.isfinite(value):
            raise CanonicalJsonError("numeric", "nonfinite binary64 value")
        return value
    if type(value) is list:
        return [_prepare(item) for item in value]
    if type(value) is dict:
        result: dict[str, object] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise CanonicalJsonError("shape", "object names must be strings")
            result[_string(key)] = _prepare(item)
        return result
    raise CanonicalJsonError("shape", "identity input must contain JSON values")


def canonical_json_bytes(value: object) -> bytes:
    """Serialize admitted JSON values with RFC 8785 without Unicode normalization."""

    try:
        return rfc8785.dumps(cast(Any, _prepare(value)))
    except (rfc8785.CanonicalizationError, UnicodeEncodeError, RecursionError) as exc:
        raise CanonicalJsonError("encoding", str(exc)) from exc


def canonical_json_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


class _Number(str):
    """Retain a JSON number lexeme until its semantic domain is known."""


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise CanonicalJsonError("duplicate", "duplicate decoded object name")
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    raise CanonicalJsonError("numeric", f"{value} is not a JSON number")


def _adapt_number(value: _Number) -> int | float:
    try:
        exact = Decimal(value)
        number = float(value)
    except (ValueError, OverflowError, InvalidOperation) as exc:
        raise CanonicalJsonError("numeric", "invalid JSON number") from exc
    if not math.isfinite(number) or (number == 0 and exact != 0):
        raise CanonicalJsonError("numeric", "number overflows or underflows binary64")
    if exact == exact.to_integral_value():
        return _exact_integer(int(exact))
    return number


def parse_identity_json(raw: bytes) -> JsonValue:
    """Parse UTF-8 JSON before duplicate keys or exact number lexemes are lost.

    JSON fractional numbers use JCS's binary64 domain. Exact unbounded values
    use named string codecs; integral tokens must survive JCS without loss.
    """

    if type(raw) is not bytes:
        raise CanonicalJsonError("shape", "identity input must be UTF-8 bytes")
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_int=_Number,
            parse_float=_Number,
            parse_constant=_reject_constant,
        )
    except UnicodeDecodeError as exc:
        raise CanonicalJsonError("unicode", "invalid UTF-8") from exc
    except (json.JSONDecodeError, RecursionError) as exc:
        raise CanonicalJsonError("syntax", "invalid JSON text") from exc

    def adapt(node: object) -> JsonValue:
        if type(node) is _Number:
            return _adapt_number(node)
        if node is None or type(node) is bool:
            return cast(JsonValue, node)
        if type(node) is str:
            return _string(node)
        if type(node) is list:
            return [adapt(item) for item in node]
        if type(node) is dict:
            return {_string(key): adapt(item) for key, item in node.items()}
        raise CanonicalJsonError("shape", "parsed value is outside JSON")

    try:
        return adapt(value)
    except RecursionError as exc:
        raise CanonicalJsonError("syntax", "excessively nested JSON text") from exc


def require_canonical_json(raw: bytes) -> JsonValue:
    value = parse_identity_json(raw)
    if canonical_json_bytes(value) != raw:
        raise CanonicalJsonError("canonical", "JSON text does not use RFC 8785 encoding")
    return value


def _decimal_pattern(maximum: int | None) -> str:
    end = r"(?![\s\S])"
    if maximum is None:
        return r"^(?:0|[1-9][0-9]*)" + end
    bound = str(maximum)
    alternatives = ["0"]
    if len(bound) > 1:
        alternatives.append(r"[1-9][0-9]{0," + str(len(bound) - 2) + "}")
    for index, character in enumerate(bound):
        lower, upper = (1 if index == 0 else 0), int(character) - 1
        if lower <= upper:
            alternatives.append(
                bound[:index] + f"[{lower}-{upper}]" + f"[0-9]{{{len(bound) - index - 1}}}"
            )
    return "^(?:" + "|".join([*alternatives, bound]) + ")" + end


def scalar_schema(domain: ScalarDomain) -> dict[str, object]:
    """Return a JSON Schema for one exact integer string representation."""

    if domain == "sequence256":
        return {"type": "string", "pattern": r"^[0-9a-f]{64}(?![\s\S])"}
    if domain == "sequence63":
        return {"type": "string", "pattern": _decimal_pattern(SEQUENCE63_MAX)}
    if domain == "nonnegative":
        return {"type": "string", "pattern": _decimal_pattern(None)}
    raise ValueError("unknown scalar domain")


def parse_scalar(domain: ScalarDomain, value: object) -> int:
    schema = scalar_schema(domain)
    if type(value) is not str or re.fullmatch(str(schema["pattern"]), value) is None:
        raise CanonicalJsonError("scalar", "invalid canonical integer string")
    if domain == "sequence256":
        return int(value, 16)
    result = 0
    for start in range(0, len(value), 9):
        chunk = value[start : start + 9]
        result = result * (10 ** len(chunk)) + int(chunk)
    return result


def format_scalar(domain: ScalarDomain, value: int) -> str:
    scalar_schema(domain)
    maximum = {"sequence63": SEQUENCE63_MAX, "sequence256": SEQUENCE256_MAX}.get(domain)
    if type(value) is not int or value < 0 or (maximum is not None and value > maximum):
        raise CanonicalJsonError("scalar", "integer is outside the declared domain")
    if domain == "sequence256":
        return f"{value:064x}"
    chunks: list[int] = []
    while value:
        value, chunk = divmod(value, 10**9)
        chunks.append(chunk)
    if not chunks:
        return "0"
    return str(chunks[-1]) + "".join(f"{part:09d}" for part in reversed(chunks[:-1]))


__all__ = [
    "CanonicalJsonError",
    "JsonValue",
    "SAFE_INTEGER",
    "SEQUENCE63_MAX",
    "SEQUENCE256_MAX",
    "ScalarDomain",
    "canonical_json_bytes",
    "canonical_json_sha256",
    "format_scalar",
    "parse_identity_json",
    "parse_scalar",
    "require_canonical_json",
    "scalar_schema",
]
