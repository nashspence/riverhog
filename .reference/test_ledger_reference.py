# SPDX-FileCopyrightText: 2026 Nash Spence
# SPDX-License-Identifier: Apache-2.0
"""Synthetic checks only; no NiCad, age, database, cloud or legal qualification."""
import json
import unittest
from dataclasses import replace
from pathlib import Path

import ledger_reference as ref


class ReferenceTests(unittest.TestCase):
    def identity(self):
        return {**{name: "a" * 64 for name in ref.DIGEST_FIELDS},
                "upstream_repository_id": 1, "candidate_repository_id": 2}

    def test_key_is_independent_of_mapping_order(self):
        value = self.identity()
        self.assertEqual(ref.work_key(value), ref.work_key(dict(reversed(list(value.items())))))

    def test_every_input_changes_work_key(self):
        value = self.identity()
        for field in value:
            changed = dict(value)
            changed[field] = 3 if field in ref.ID_FIELDS else "b" * 64
            with self.subTest(field=field):
                self.assertNotEqual(ref.work_key(value), ref.work_key(changed))

    def test_timestamp_and_ciphertext_are_not_work_identity(self):
        for name in ("timestamp", "ciphertext_sha256", "attempt"):
            value = self.identity()
            value[name] = 1
            with self.assertRaises(ValueError):
                ref.work_key(value)

    def test_invalid_identity_is_rejected(self):
        for bad in (True, 0, -1, "2", 2**63):
            value = self.identity()
            value["candidate_repository_id"] = bad
            with self.assertRaises(ValueError):
                ref.work_key(value)
        value = self.identity()
        value["profiles_sha256"] = "bad"
        with self.assertRaises(ValueError):
            ref.work_key(value)

    def test_round_trip_across_boundaries(self):
        for size in (1, ref.CHUNK_BYTES - 1, ref.CHUNK_BYTES, ref.CHUNK_BYTES + 1,
                     ref.CHUNK_BYTES * 3 + 9):
            raw = b"x" * size
            self.assertEqual(raw, ref.reassemble(ref.split_ciphertext(raw), ref.sha256(raw), size))

    def test_missing_duplicate_and_reordered_chunks_fail(self):
        raw = b"a" * ref.CHUNK_BYTES + b"b" * ref.CHUNK_BYTES
        chunks = ref.split_ciphertext(raw)
        for invalid in (chunks[:-1], (chunks[0], chunks[0]), tuple(reversed(chunks))):
            with self.assertRaises(ValueError):
                ref.reassemble(invalid, ref.sha256(raw), len(raw))

    def test_corrupt_chunk_and_whole_digest_fail(self):
        raw = b"payload"
        chunks = ref.split_ciphertext(raw)
        with self.assertRaises(ValueError):
            ref.reassemble((replace(chunks[0], payload=b"PAYLOAD"),), ref.sha256(raw), len(raw))
        with self.assertRaises(ValueError):
            ref.reassemble(chunks, "0" * 64, len(raw))

    def test_repacked_wrong_bytes_fail_even_with_new_chunk_digests(self):
        raw = b"a" * ref.CHUNK_BYTES + b"b" * ref.CHUNK_BYTES
        changed = b"b" * ref.CHUNK_BYTES + b"a" * ref.CHUNK_BYTES
        with self.assertRaises(ValueError):
            ref.reassemble(ref.split_ciphertext(changed), ref.sha256(raw), len(raw))

    def test_length_limits(self):
        with self.assertRaises(ValueError):
            ref.split_ciphertext(b"")
        with self.assertRaises(ValueError):
            ref.split_ciphertext(b"x" * (ref.MAX_CIPHERTEXT_BYTES + 1))
        with self.assertRaises(ValueError):
            ref.reassemble((), "a" * 64, True)

    def test_batches_account_for_base64_overhead(self):
        chunks = ref.split_ciphertext(b"x" * ref.CHUNK_BYTES * ref.BATCH_ROWS)
        encoded = ref.encoded_batch(chunks)
        self.assertGreater(len(encoded), ref.CHUNK_BYTES * ref.BATCH_ROWS)
        self.assertLess(len(encoded), ref.MAX_BATCH_JSON_BYTES)
        for row in json.loads(encoded):
            self.assertLess(len(json.dumps(row).encode()), 64 * 1024)
        with self.assertRaises(ValueError):
            ref.encoded_batch(chunks + (chunks[0],))

    def test_partial_or_unverified_work_is_not_reused(self):
        for status in ("matches", "no_match", "failed", "partial", "deferred", "running"):
            for coverage in (False, True):
                for custody in (False, True):
                    expected = status in {"matches", "no_match"} and coverage and custody
                    self.assertEqual(expected, ref.reusable(result=status,
                        coverage_complete=coverage, custody_verified=custody))

    def test_contract_agrees_with_reference(self):
        value = json.loads(Path(__file__).with_name("CONTRACT.json").read_text())
        self.assertEqual(value["scheduling"], "post-v1-only")
        self.assertFalse(value["deployment_authorized"])
        self.assertEqual(value["chunk_bytes"], ref.CHUNK_BYTES)
        self.assertEqual(value["batch_rows"], ref.BATCH_ROWS)
        self.assertEqual(set(value["work_identity_fields"]), ref.DIGEST_FIELDS | ref.ID_FIELDS)
        profiles = value["nicad_profiles"]
        self.assertEqual([(p["rename"], p["threshold"]) for p in profiles],
                         [("none", "0.00"), ("blind", "0.00"), ("blind", "0.20")])


if __name__ == "__main__":
    unittest.main()
