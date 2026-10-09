"""Count publication work at increasing cardinality; not an end-to-end timing claim.

Run from the locked repository workspace. The generic producer and provenance
transfer are real; the API is a deterministic test double with no network delay.
Keep raw request counts separate from wall time and from final-image evidence.
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from riverhog_client.processing.history_transfer import CanonicalHistoryTransfer

from tests.unit.test_history_prefix_reuse import PrefixApi, anchor
from tests.unit.test_incremental_collection_producer import _producer
from tests.unit.test_incremental_custody_polling import OpenPackApi, append_member


def measure(audio_files: int, *, seal_each_member: bool) -> dict[str, object]:
    members = 2 * audio_files + 1
    api = OpenPackApi(pack_members=1 if seal_each_member else 10000)
    producer = _producer(api)
    started = time.perf_counter()
    try:
        with TemporaryDirectory(prefix="publication-count-") as temporary:
            root = Path(temporary)
            for index in range(members):
                path, _, receipts = append_member(producer, root, index)
                assert not receipts
                assert not producer.reconcile_custody()
                assert path.exists(), "receipt-free local bytes must never be released"
    finally:
        producer.stop()
    return {
        "audio_files_equivalent": audio_files,
        "output_members": members,
        "case": "sealed-history-pending" if seal_each_member else "open-pack",
        "registration_calls": api.registration_calls,
        "registered_member_rows": api.registered_members,
        "receipt_get_calls": len(api.reads),
        "elapsed_seconds": time.perf_counter() - started,
        "early_receipts": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    results = [
        measure(count, seal_each_member=sealed)
        for sealed in (False, True)
        for count in (16, 32, 64, 128)
    ]
    prefix, selected = anchor()
    api = PrefixApi(prefix)
    transfer = CanonicalHistoryTransfer(api, 1)
    for _ in range(128):
        transfer._journal(None, selected)
    document = {
        "format": "riverhog-publication-count-reference/v1",
        "head_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "python": platform.python_version(),
        "runtime_blob_ids": {
            name: subprocess.check_output(["git", "hash-object", name], text=True).strip()
            for name in (
                "packages/riverhog-client/src/riverhog_client/producer.py",
                "packages/riverhog-client/src/riverhog_client/processing/history_transfer.py",
                "packages/riverhog-client/src/riverhog_client/processing/writer.py",
            )
        },
        "scope": "native implementation with deterministic API doubles; no Docker/network/codec",
        "publication": results,
        "repeated_receiver_prefix": {
            "requests": 128,
            "status_reads": api.status_reads,
            "prefix_streams": api.stream_reads,
            "unselected_tail_chunks_read": api.suffix_reads,
        },
    }
    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    print(json.dumps(document, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
