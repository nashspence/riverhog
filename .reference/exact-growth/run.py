# SPDX-License-Identifier: Apache-2.0
"""Run reference diagnostics and record observations; no product claims."""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import time
import unittest
from pathlib import Path

import test_contract

ROOT = Path(__file__).resolve().parents[2]


def report() -> tuple[dict, bool]:
    suite = unittest.defaultTestLoader.loadTestsFromModule(test_contract)
    start = time.perf_counter()
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    elapsed = time.perf_counter() - start
    p = test_contract.old_probe
    k = p.load_tag_kernel()
    store = p.counting_store(k)
    old = k.CollectionTagSet(store).insert("L" * 65536)
    store.counts.clear()
    two = old.insert("x")
    insert = dict(store.counts)
    store.counts.clear()
    restored = two.discard("x")
    remove = dict(store.counts)
    if restored.root != old.root:
        raise AssertionError("native tag historical identity changed")
    chunks = [b"a" * 64 for _ in range(128)]
    a, slow = p.prefix_hashes(chunks, incremental=False)
    b, fast = p.prefix_hashes(chunks, incremental=True)
    if a != b:
        raise AssertionError("prefix identities changed")
    files = [".reference/EXACT_GROWTH.md", ".reference/exact-growth/basis.py",
             ".reference/exact-growth/test_contract.py", ".reference/exact-growth/run.py",
             ".reference/growth/probe.py", ".reference/annotation_reference.py", p.TAG_PATH]
    hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in files}
    result_data = {
        "format": "reference-exact-growth-observations/v0",
        "production_conformance": False,
        "reference_parent": "ce0d34fa8da9e423adcf681c30f518d5949042f8",
        "audited_main": "9a485bc174b7b8d703ac05159e3df9be4f36be98",
        "environment": {"python": platform.python_version(), "pinned_environment": False},
        "source_sha256": hashes,
        "tests": {"run": result.testsRun, "failures": len(result.failures),
                  "errors": len(result.errors), "skipped": len(result.skipped),
                  "passed": result.wasSuccessful(), "wall_seconds": elapsed},
        "native_tag_slice": {"operand_octets": 1, "surviving_tag_octets": 65536,
                             "insert": insert, "remove": remove,
                             "old_root_preserved": True},
        "exact_prefix_hash": {"checkpoints": 128, "selected_octets": 8192,
                              "incremental_hash_input_octets": fast,
                              "repeated_prefix_hash_input_octets": slow,
                              "every_anchor_equal": a == b},
        "limitations": [
            "Arithmetic bases are normalized class expressions, not measured product ceilings.",
            "Finite samples do not establish asymptotic proofs.",
            "Native tag execution uses the previous exact source-pinned AST slice, not the full module/backend.",
            "Generated-profile shape check is not canonical runtime validation.",
            "Prefix and counting-sink tests do not prove storage, encryption, authorization or installed recovery.",
            "No complete pinned toolchain, full product gates, or new backend conformance is claimed.",
        ],
    }
    return result_data, result.wasSuccessful()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    data, passed = report()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n")
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
