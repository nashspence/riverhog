"""Run the maintained collector over this reference's exact executed witnesses."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from riverhog_canonical_json import canonical_json_bytes  # noqa: E402

from scripts.validation_identity import source_identity  # noqa: E402
from scripts.validation_witnesses import (  # noqa: E402
    collect_test_witnesses,
    discover_test_witnesses,
)

FILES = {
    "packages/riverhog-provenance/tests/test_corpus_primary_selection.py",
    "tests/unit/test_primary_production_work.py",
    "tests/unit/test_shared_primary_admission.py",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = source_identity(ROOT)
    if source.get("source_clean") is not True:
        raise SystemExit("Commit the exact reference tree before collecting executed evidence.")
    if args.output.exists():
        raise SystemExit("Use a new output path; an earlier execution must remain visible.")
    expected = tuple(
        witness.id
        for witness in discover_test_witnesses(ROOT)
        if witness.node_id.split("::", 1)[0] in FILES
    )
    paths = tuple((args.evidence / "pytest").glob(f"*/{args.run_id}/*/*.json"))
    records: list[dict[str, object]] = []
    results = collect_test_witnesses(
        paths,
        root=ROOT,
        expected=expected,
        source_sha=str(source["source_sha"]),
        run_id=args.run_id,
        evidence=records,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(
        canonical_json_bytes(
            {
                "format": "riverhog-growth-execution-bundle/v1",
                "source": source,
                "run_id": args.run_id,
                "results": results,
                "records": records,
            }
        )
    )
    print(f"Verified {len(results)} source-owned witnesses and {len(records)} executions.")
    for identity, result in results.items():
        print(f"{identity}: {result}")


if __name__ == "__main__":
    main()
