"""NON-AUTHORITATIVE #897 ON-01/02 experiment; not a production wire contract.

JCS is delegated to rfc8785. This module prototypes admission and scalar codecs,
not another serializer. See README.md for the exact-integer/binary64 distinction.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
from decimal import Decimal, InvalidOperation

import rfc8785

BASE = "1b6ee5e8a2f1df39852465050df3efa96d6773e4"
SAFE_INTEGER = (1 << 53) - 1
SEQUENCE63_MAX = (1 << 63) - 1
SEQUENCE256_MAX = (1 << 256) - 1


class Rejected(ValueError):
    """Stable category for executable rejection vectors, not an HTTP error API."""

    def __init__(self, category: str, detail: str) -> None:
        self.category = category
        super().__init__(f"{category}: {detail}")


def _string(value: str) -> str:
    for char in value:
        code = ord(char)
        if 0xD800 <= code <= 0xDFFF or 0xFDD0 <= code <= 0xFDEF or code & 0xFFFF >= 0xFFFE:
            raise Rejected("unicode", "I-JSON excludes surrogates and noncharacters")
    return value  # Never normalize Unicode, paths, escapes, or string subtypes.


def _exact_integer(value: int) -> int | float:
    if -SAFE_INTEGER <= value <= SAFE_INTEGER:
        return value
    try:
        number = float(value)
    except OverflowError as exc:
        raise Rejected("numeric", "exact integer requires a declared string codec") from exc
    if not math.isfinite(number) or int(number) != value:
        raise Rejected("numeric", "integer is not exactly representable as binary64")
    # Binary64 exactness alone is insufficient for exact INTEGER consumers:
    # 2**63 is exact, but its JCS spelling is 9223372036854776000.
    if Decimal(rfc8785.dumps(number).decode("ascii")) != value:
        raise Rejected("numeric", "JCS decimal spelling would change the exact integer")
    return number  # Checked bridge past this library's safe-Python-int restriction.


def _prepare(value: object, depth: int = 0) -> object:
    if depth > 64:
        raise Rejected("budget", "prototype nesting budget exceeded")
    if value is None or type(value) is bool:
        return value
    if type(value) is str:
        return _string(value)
    if type(value) is int:
        return _exact_integer(value)
    if type(value) is float:
        if not math.isfinite(value):
            raise Rejected("numeric", "nonfinite binary64")
        return value  # A supplied float already declares binary64 semantics.
    if type(value) is list:
        return [_prepare(item, depth + 1) for item in value]
    if type(value) is dict:
        result = {}
        for key, item in value.items():
            if type(key) is not str:
                raise Rejected("shape", "object names must be strings")
            result[_string(key)] = _prepare(item, depth + 1)
        return result
    raise Rejected("shape", "use JSON values and explicit domain codecs, not implicit coercion")


def canonical_json_bytes(value: object) -> bytes:
    """JCS for admitted values; floats mean binary64, integers mean exact integers."""
    try:
        return rfc8785.dumps(_prepare(value))  # type: ignore[arg-type]
    except (rfc8785.CanonicalizationError, UnicodeEncodeError) as exc:
        raise Rejected("encoding", str(exc)) from exc


def canonical_json_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


class _Number(str):
    """Keep original number lexemes until the semantic domain is selected."""


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise Rejected("duplicate", "duplicate decoded object name")
        result[key] = value
    return result


def _constant(_: str) -> object:
    raise Rejected("numeric", "NaN and Infinity are not JSON numbers")


def _pointer(parent: str, component: str) -> str:
    return parent + "/" + component.replace("~", "~0").replace("/", "~1")


def parse_identity_json(
    raw: bytes,
    *,
    binary64_paths: frozenset[str] = frozenset(),
    max_bytes: int = 1024 * 1024,
) -> object:
    """Reject information loss BEFORE dict/float conversion.

    The default numeric domain is exact, including exact decimal fractions.
    Only schema-selected JSON Pointer paths may opt into binary64 rounding.
    Thus decimal 0.1 needs an explicit binary64 path or an exact decimal string.
    This is a proposed admission cut, not a claim that RFC 8785 rejects 0.1.
    Caller budgets are carrier bounds, not semantic limits on logical totals.
    """
    if type(raw) is not bytes:
        raise Rejected("shape", "the parsing boundary requires UTF-8 bytes")
    if len(raw) > max_bytes:
        raise Rejected("budget", "prototype input byte budget exceeded")
    if raw.startswith(b"\xef\xbb\xbf"):
        raise Rejected("syntax", "BOM is not admitted")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique,
                           parse_int=_Number, parse_float=_Number, parse_constant=_constant)
    except UnicodeDecodeError as exc:
        raise Rejected("unicode", "invalid UTF-8") from exc
    except (json.JSONDecodeError, RecursionError) as exc:
        raise Rejected("syntax", "invalid or excessively nested JSON") from exc

    def adapt(node: object, path: str = "", depth: int = 0) -> object:
        if depth > 64:
            raise Rejected("budget", "prototype nesting budget exceeded")
        if type(node) is _Number:
            try:
                exact = Decimal(node)
                number = float(node)
            except (ValueError, OverflowError, InvalidOperation) as exc:
                raise Rejected("numeric", "invalid number") from exc
            if not math.isfinite(number) or (number == 0 and exact != 0):
                raise Rejected("numeric", "overflow or nonzero underflow")
            if path in binary64_paths:
                return number
            if Decimal.from_float(number) != exact:
                raise Rejected("numeric", "exact value needs a string or declared binary64 domain")
            if exact == exact.to_integral_value():
                return _exact_integer(int(exact))
            if Decimal(rfc8785.dumps(number).decode("ascii")) != exact:
                raise Rejected("numeric", "JCS spelling would change the exact decimal value")
            return number
        if type(node) is list:
            return [adapt(item, _pointer(path, str(i)), depth + 1) for i, item in enumerate(node)]
        if type(node) is dict:
            return {_string(key): adapt(item, _pointer(path, key), depth + 1)
                    for key, item in node.items()}
        return _prepare(node, depth)

    return adapt(value)


def require_canonical_json(raw: bytes, **kwargs: object) -> object:
    value = parse_identity_json(raw, **kwargs)  # type: ignore[arg-type]
    if canonical_json_bytes(value) != raw:
        raise Rejected("canonical", "valid JSON is not necessarily canonical JSON")
    return value


def _decimal_pattern(maximum: int | None) -> str:
    # (?![\s\S]) is an absolute end assertion in both Python and ECMAScript;
    # '$' alone would also match before a final newline in both engines.
    end = r"(?![\s\S])"
    if maximum is None:
        return r"^(?:0|[1-9][0-9]*)" + end
    text = str(maximum)
    choices = ["0"]
    if len(text) > 1:
        choices.append(r"[1-9][0-9]{0," + str(len(text) - 2) + "}")
    for i, char in enumerate(text):
        lower, upper = (1 if i == 0 else 0), int(char) - 1
        if lower <= upper:
            choices.append(text[:i] + f"[{lower}-{upper}]" + f"[0-9]{{{len(text)-i-1}}}")
    return "^(?:" + "|".join(choices + [text]) + ")" + end


def scalar_schema(domain: str) -> dict[str, object]:
    if domain == "sequence256":
        return {"type": "string", "pattern": r"^[0-9a-f]{64}(?![\s\S])"}
    if domain in {"sequence63", "nonnegative"}:
        maximum = SEQUENCE63_MAX if domain == "sequence63" else None
        return {"type": "string", "pattern": _decimal_pattern(maximum)}
    raise ValueError("unknown prototype scalar domain")


def parse_scalar(domain: str, value: object) -> int:
    schema = scalar_schema(domain)
    if type(value) is not str or re.fullmatch(str(schema["pattern"]), value) is None:
        raise Rejected("scalar", "not the declared canonical string representation")
    if domain == "sequence256":
        return int(value, 16)
    # No Python decimal digit-conversion ceiling added to unbounded totals.
    result = 0
    for i in range(0, len(value), 9):
        chunk = value[i:i + 9]
        result = result * (10 ** len(chunk)) + int(chunk)
    return result


def format_scalar(domain: str, value: int) -> str:
    scalar_schema(domain)  # Validate domain even for zero.
    maximum = {"sequence63": SEQUENCE63_MAX, "sequence256": SEQUENCE256_MAX}.get(domain)
    if type(value) is not int or value < 0 or (maximum is not None and value > maximum):
        raise Rejected("scalar", "not an integer in the declared semantic domain")
    if domain == "sequence256":
        return f"{value:064x}"
    chunks = []
    while value:
        value, chunk = divmod(value, 10**9)
        chunks.append(chunk)
    return str(chunks[-1]) + "".join(f"{part:09d}" for part in reversed(chunks[:-1])) if chunks else "0"


def recorded_json_sha256(recorded_json_text: bytes) -> str:
    """Hash an ALREADY EXTRACTED entry's exact bytes, never JCS reserialization.

    Caller owns RFC 7464 framing and the existing journal schema/chain checks.
    This function is only a hash-domain guard, NOT a replacement journal verifier.
    """
    if type(recorded_json_text) is not bytes:
        raise Rejected("shape", "recorded JSON text must remain bytes")
    return hashlib.sha256(recorded_json_text).hexdigest()


def verify_recorded_json(recorded_json_text: bytes, expected_sha256: str) -> bool:
    return hmac.compare_digest(recorded_json_sha256(recorded_json_text), expected_sha256)


def upload_volume_schema() -> dict[str, object]:
    """ON-02 shape prototype; the consumer separately verifies the ID binding."""
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object", "additionalProperties": False,
        "required": ["kind", "sequence", "volume_id"],
        "properties": {
            "kind": {"enum": ["pack", "segment"]},
            "sequence": scalar_schema("sequence256"),
            "volume_id": {"type": "string", "pattern": r"^(?:pack|segment)-[0-9a-f]{64}(?![\s\S])"},
        },
    }


def emit_upload_volume(kind: str, sequence: int) -> bytes:
    """Producer slice of CollectionUploadVolumeSummaryDocument, not its replacement."""
    if kind not in ("pack", "segment"):
        raise Rejected("binding", "unsupported volume kind")
    ordinal = format_scalar("sequence256", sequence)
    return canonical_json_bytes({"kind": kind, "sequence": ordinal,
                                 "volume_id": kind + "-" + ordinal})


def read_upload_volume(raw: bytes) -> tuple[str, int]:
    """Strict raw parser -> string codec -> existing kind/sequence ID invariant."""
    document = require_canonical_json(raw)
    if type(document) is not dict or set(document) != {"kind", "sequence", "volume_id"}:
        raise Rejected("shape", "invalid volume summary fields")
    kind = document["kind"]
    sequence = parse_scalar("sequence256", document["sequence"])
    if kind not in ("pack", "segment") or document["volume_id"] != kind + "-" + document["sequence"]:
        raise Rejected("binding", "volume ID differs from kind and sequence")
    return kind, sequence
