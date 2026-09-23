"""Executable #897 reference vectors. No production modules are imported or patched."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import random
import struct
import subprocess
import unittest
from decimal import Decimal

from jsonschema import Draft202012Validator

from prototype import (
    Rejected, SEQUENCE63_MAX, SEQUENCE256_MAX, canonical_json_bytes,
    canonical_json_sha256, emit_upload_volume, format_scalar, parse_identity_json,
    parse_scalar, read_upload_volume, recorded_json_sha256, require_canonical_json,
    scalar_schema, upload_volume_schema, verify_recorded_json,
)

HERE = Path(__file__).resolve().parent
VECTORS = json.loads((HERE / "vectors.json").read_text(encoding="utf-8"))


def raw_bytes(case: dict) -> bytes:
    return bytes.fromhex(case["input_hex"]) if "input_hex" in case else case["input"].encode("utf-8")


class ReferenceTests(unittest.TestCase):
    def rejected(self, category, function, *args, **kwargs):
        with self.assertRaises(Rejected) as caught:
            function(*args, **kwargs)
        self.assertEqual(category, caught.exception.category)

    def test_json_vectors(self):
        for case in VECTORS["json"]:
            with self.subTest(id=case["id"]):
                raw = raw_bytes(case)
                options = {"binary64_paths": frozenset(case.get("binary64_paths", []))}
                if "reject" in case:
                    self.rejected(case["reject"], parse_identity_json, raw, **options)
                    continue
                value = parse_identity_json(raw, **options)
                expected = case["canonical"].encode("utf-8")
                self.assertEqual(expected, canonical_json_bytes(value))
                self.assertEqual(case["sha256"], canonical_json_sha256(value))
                self.assertEqual(expected, canonical_json_bytes(require_canonical_json(expected, **options)))
                if raw != expected:
                    self.rejected("canonical", require_canonical_json, raw, **options)

    def test_rfc8785_appendix_b(self):
        for case in VECTORS["binary64"]:
            with self.subTest(bits=case["bits"]):
                number = struct.unpack(">d", bytes.fromhex(case["bits"]))[0]
                if "reject" in case:
                    self.rejected("numeric", canonical_json_bytes, number)
                else:
                    self.assertEqual(case["canonical"].encode("ascii"), canonical_json_bytes(number))

    def test_scalar_parser_schema_agreement(self):
        for case in VECTORS["scalars"]:
            with self.subTest(id=case["id"]):
                schema = scalar_schema(case["domain"])
                Draft202012Validator.check_schema(schema)
                self.assertEqual(case["accept"], Draft202012Validator(schema).is_valid(case["value"]))
                if case["accept"]:
                    value = parse_scalar(case["domain"], case["value"])
                    self.assertEqual(case["value"], format_scalar(case["domain"], value))
                else:
                    self.rejected("scalar", parse_scalar, case["domain"], case["value"])
                # The schema itself is JCS-admissible; no imprecise huge maximum.
                require_canonical_json(canonical_json_bytes(schema))

    def test_scalar_boundaries_and_full_domains(self):
        rng = random.Random(897)
        for domain, maximum in [("sequence63", SEQUENCE63_MAX), ("sequence256", SEQUENCE256_MAX)]:
            values = [0, 1, (1 << 53) - 1, 1 << 53, (1 << 53) + 1, maximum]
            values += [rng.randrange(maximum + 1) for _ in range(100)]
            for n in values:
                text = format_scalar(domain, n)
                self.assertIs(type(text), str)
                self.assertEqual(n, parse_scalar(domain, text))
                self.assertTrue(Draft202012Validator(scalar_schema(domain)).is_valid(text))
            for n in [-1, maximum + 1, True, 1.0]:
                self.rejected("scalar", format_scalar, domain, n)
        # No new decimal-conversion ceiling or value-dependent string/number switch.
        text = "1" + "0" * 5000
        value = parse_scalar("nonnegative", text)
        self.assertEqual(text, format_scalar("nonnegative", value))
        self.assertTrue(Draft202012Validator(scalar_schema("nonnegative")).is_valid(text))
        self.assertEqual(b'"' + text.encode() + b'"', canonical_json_bytes(text))

    def test_upload_producer_schema_consumer_slice(self):
        schema = upload_volume_schema()
        Draft202012Validator.check_schema(schema)
        for kind in ("pack", "segment"):
            for n in (0, 1, (1 << 53) + 1, SEQUENCE256_MAX):
                raw = emit_upload_volume(kind, n)
                document = parse_identity_json(raw)
                Draft202012Validator(schema).validate(document)
                self.assertEqual((kind, n), read_upload_volume(raw))
                self.assertEqual(format_scalar("sequence256", n), document["sequence"])
                self.assertEqual(kind + "-" + document["sequence"], document["volume_id"])
        # No integer/string input union; even small numbers are rejected on this wire.
        document = json.loads(emit_upload_volume("pack", 0))
        document["sequence"] = 0
        self.assertFalse(Draft202012Validator(schema).is_valid(document))
        self.rejected("scalar", read_upload_volume, canonical_json_bytes(document))
        document = json.loads(emit_upload_volume("pack", 0))
        document["volume_id"] = "segment-" + "0" * 64
        # Shape alone cannot prove the existing cross-field identity invariant.
        self.assertTrue(Draft202012Validator(schema).is_valid(document))
        self.rejected("binding", read_upload_volume, canonical_json_bytes(document))
        self.rejected("binding", emit_upload_volume, "other", 0)

    def test_new_sequence_projection_not_legacy_rewrite(self):
        # This is a field projection, NOT a complete journal or a migration utility.
        current = SEQUENCE63_MAX
        new_projection = {
            "sequence": format_scalar("sequence63", current),
            "previous_entry": {"sequence": format_scalar("sequence63", current - 1)},
        }
        raw = canonical_json_bytes(new_projection)
        parsed = require_canonical_json(raw)
        self.assertEqual(current, parse_scalar("sequence63", parsed["sequence"]))
        self.assertEqual(current - 1, parse_scalar("sequence63", parsed["previous_entry"]["sequence"]))
        self.assertEqual('0', format_scalar("sequence63", 0))

    def test_opaque_schema_and_number_domains(self):
        # Never change a JSON Schema numeric keyword to a string in place.
        big_schema = {"type": "integer", "maximum": SEQUENCE63_MAX}
        self.rejected("numeric", canonical_json_bytes, big_schema)
        self.assertEqual(SEQUENCE63_MAX, big_schema["maximum"])
        self.rejected("numeric", parse_identity_json, b'{"opaque":{"n":0.1}}')
        value = parse_identity_json(b'{"opaque":{"n":0.1}}', binary64_paths=frozenset({"/opaque/n"}))
        self.assertEqual(b'{"opaque":{"n":0.1}}', canonical_json_bytes(value))
        # Already-parsed floats cannot recover original lexemes or detect prior rounding.
        self.assertEqual(b'9007199254740992', canonical_json_bytes(float(9007199254740993)))
        self.rejected("numeric", parse_identity_json, b'9007199254740993')
        # Exactly binary64 does not imply an unchanged decimal spelling (RFC App. B).
        self.rejected("numeric", parse_identity_json, b'1424953923781206.25')
        self.assertEqual(b'1424953923781206.2', canonical_json_bytes(1424953923781206.25))
        self.rejected("numeric", canonical_json_bytes, 1 << 63)
        self.assertEqual(b'9223372036854776000', canonical_json_bytes(float(1 << 63)))

    def test_exact_recorded_bytes_are_not_canonical_value_identity(self):
        a = b'{"b":2, "a":"\\u0061"}'
        b = b'{"a":"a","b":2}'
        self.assertEqual(canonical_json_bytes(parse_identity_json(a)), b)
        self.assertNotEqual(recorded_json_sha256(a), recorded_json_sha256(b))
        self.assertTrue(verify_recorded_json(a, hashlib.sha256(a).hexdigest()))
        self.assertFalse(verify_recorded_json(b, hashlib.sha256(a).hexdigest()))
        # Caller has already extracted JSON text; framing is not part of entry hash.
        frame = b'\x1e' + a + b'\n'
        self.assertEqual(recorded_json_sha256(a), recorded_json_sha256(frame[1:-1]))
        self.assertNotEqual(recorded_json_sha256(a), recorded_json_sha256(frame))
        # This byte-domain guard does not impose a new numeric parser on inherited bytes.
        inherited = b'{"sequence":9223372036854775807}'
        before = inherited[:]
        self.assertTrue(verify_recorded_json(inherited, hashlib.sha256(inherited).hexdigest()))
        self.assertEqual(before, inherited)
        self.rejected("numeric", parse_identity_json, inherited)

    def test_recipe_boundaries_are_not_changed_by_jcs(self):
        self.assertNotEqual(canonical_json_sha256({}), canonical_json_sha256({"x": None}))
        self.assertNotEqual(canonical_json_sha256([1, 2]), canonical_json_sha256([2, 1]))
        self.assertNotEqual(canonical_json_sha256("é"), canonical_json_sha256("é"))
        # Existing 8-byte big-endian length-prefix ordered-set recipe remains intact.
        members = [{"volume_id": "pack-" + "0" * 64, "sealed_receipt_sha256": "a" * 64},
                   {"volume_id": "segment-" + "0" * 64, "sealed_receipt_sha256": "b" * 64}]
        encoded = [canonical_json_bytes(item) for item in members]
        framed = b''.join(len(item).to_bytes(8, "big") + item for item in encoded)
        self.assertNotEqual(hashlib.sha256(framed).hexdigest(), canonical_json_sha256(members))
        self.assertNotEqual(hashlib.sha256(framed).hexdigest(), hashlib.sha256(b''.join(encoded)).hexdigest())

    def test_shape_encoding_and_budgets(self):
        for value in [Decimal("0.5"), (1,), {1: "x"}, b'{}', {"set"}, object()]:
            self.rejected("shape", canonical_json_bytes, value)
        for value in [math.inf, -math.inf, math.nan]:
            self.rejected("numeric", canonical_json_bytes, value)
        for value in ['\ud800', '\ufdd0', {"\uffff": 1}]:
            self.rejected("unicode", canonical_json_bytes, value)
        cycle = []
        cycle.append(cycle)
        self.rejected("budget", canonical_json_bytes, cycle)
        self.rejected("shape", parse_identity_json, '{}')
        self.rejected("budget", parse_identity_json, b'{}', max_bytes=1)
        self.rejected("budget", parse_identity_json, b'[' * 66 + b'0' + b']' * 66)
        self.assertEqual({}, parse_identity_json(b'{}', max_bytes=2))

    @unittest.skipUnless(os.environ.get("REFERENCE_NODE") == "1", "set REFERENCE_NODE=1 for Node oracle")
    def test_node_ecmascript_differential(self):
        accepted = [c for c in VECTORS["json"] if "canonical" in c]
        rng = random.Random(897)
        bits = [c["bits"] for c in VECTORS["binary64"]]
        bits += [f"{rng.getrandbits(64):016x}" for _ in range(10000)]
        scalars = [{"pattern": scalar_schema(c["domain"])["pattern"], "value": c["value"]}
                   for c in VECTORS["scalars"]]
        request = {"json": [c["input"] for c in accepted], "binary64": bits, "scalars": scalars}
        result = subprocess.run(["node", str(HERE / "node_oracle.mjs")],
                                input=json.dumps(request), text=True, capture_output=True,
                                check=True, timeout=30)
        actual = json.loads(result.stdout)
        self.assertEqual([c["canonical"] for c in accepted], actual["json"])
        self.assertEqual([c["accept"] for c in VECTORS["scalars"]], actual["scalars"])
        for bit_pattern, output in zip(bits, actual["binary64"], strict=True):
            number = struct.unpack(">d", bytes.fromhex(bit_pattern))[0]
            with self.subTest(bits=bit_pattern):
                if math.isfinite(number):
                    self.assertEqual(output.encode("utf-8"), canonical_json_bytes(number))
                else:
                    self.assertIsNone(output)
                    self.rejected("numeric", canonical_json_bytes, number)


if __name__ == "__main__":
    print(f"Fixture cases: {len(VECTORS['json'])} raw JSON, {len(VECTORS['binary64'])} binary64, "
          f"{len(VECTORS['scalars'])} scalar. Node differential: 10,000 seeded bit patterns when enabled.",
          flush=True)
    unittest.main(verbosity=2)
