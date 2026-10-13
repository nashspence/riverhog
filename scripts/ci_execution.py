"""Record the actual checkout around Actions-owned validation without rerunning it."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.validation_identity import source_identity

ROOT = Path(__file__).resolve().parents[1]


def execution_identity(work: str) -> dict[str, object]:
    if re.fullmatch(r"codeql-[a-z][a-z0-9-]{0,63}", work) is None:
        raise ValueError("invalid Actions work identity")
    repository = os.environ["GITHUB_REPOSITORY"]
    workflow = os.environ["GITHUB_WORKFLOW_REF"]
    sha = os.environ["GITHUB_WORKFLOW_SHA"]
    source = os.environ["GITHUB_SHA"]
    if (
        re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None
        or not workflow.startswith(repository + "/.github/workflows/codeql.yml@refs/")
        or re.fullmatch(r"[0-9a-f]{40}", sha) is None
        or re.fullmatch(r"[0-9a-f]{40}", source) is None
        or os.environ["GITHUB_JOB"] != "analyze"
    ):
        raise ValueError("checkout record lacks its Actions workflow identity")
    run, attempt = int(os.environ["GITHUB_RUN_ID"]), int(os.environ["GITHUB_RUN_ATTEMPT"])
    if min(run, attempt) < 1:
        raise ValueError("checkout record requires a positive run and attempt")
    return {
        "repository": repository,
        "workflow_ref": workflow,
        "workflow_sha": sha,
        "run_id": run,
        "run_attempt": attempt,
        "work": work,
        "github_sha": source,
    }


def write_checkout(path: Path, value: dict[str, object]) -> None:
    # This schema contains only ASCII property names, integers, booleans and strings.
    # Its compact UTF-8 JSON is also canonical JSON, without a bootstrap dependency.
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    temporary.replace(path)


def record_checkout(phase: str, work: str, output: Path, *, root: Path = ROOT) -> int:
    context = execution_identity(work)
    source = source_identity(root)
    if phase == "start":
        if output.exists():
            raise ValueError("checkout evidence cannot overwrite another invocation")
        record: dict[str, object] = {
            "format": "riverhog-actions-checkout/v1",
            "execution": context,
            "source": source,
            "state": "running",
            "started_at": datetime.now(UTC).isoformat(),
            "started_monotonic_ns": str(time.monotonic_ns()),
        }
    else:
        record = json.loads(output.read_bytes())
        if (
            record.get("format") != "riverhog-actions-checkout/v1"
            or record.get("execution") != context
        ):
            raise ValueError("checkout evidence differs from the started invocation")
        if record.get("state") != "running":
            raise ValueError("checkout invocation is already closed")
        elapsed = time.monotonic_ns() - int(str(record["started_monotonic_ns"]))
        if elapsed < 0:
            raise ValueError("checkout clock predates its start")
        record.update(
            state="completed",
            source_end=source,
            finished_at=datetime.now(UTC).isoformat(),
            elapsed_ns=str(elapsed),
        )
    write_checkout(output, record)
    return (
        int(
            source["source_clean"] is not True
            or source["source_sha"] != context["github_sha"]
            or (phase == "finish" and record["source"] != source)
        )
        * 2
    )


def validate_checkout(row: Any, *, repository: str, run_id: int, attempt: int, work: str) -> str:
    fields = {
        "format",
        "execution",
        "source",
        "source_end",
        "state",
        "started_at",
        "finished_at",
        "started_monotonic_ns",
        "elapsed_ns",
    }
    if (
        not isinstance(row, dict)
        or set(row) != fields
        or row["format"] != "riverhog-actions-checkout/v1"
    ):
        raise ValueError("invalid or incomplete checkout record")
    context, source = row["execution"], row["source"]
    if (
        not isinstance(context, dict)
        or set(context)
        != {
            "repository",
            "workflow_ref",
            "workflow_sha",
            "run_id",
            "run_attempt",
            "work",
            "github_sha",
        }
        or type(context["run_id"]) is not int
        or type(context["run_attempt"]) is not int
        or context["repository"] != repository
        or context["run_id"] != run_id
        or context["run_attempt"] != attempt
        or context["work"] != work
        or not isinstance(context["workflow_ref"], str)
        or not context["workflow_ref"].startswith(
            repository + "/.github/workflows/codeql.yml@refs/"
        )
        or re.fullmatch(r"[0-9a-f]{40}", str(context["workflow_sha"])) is None
        or not isinstance(source, dict)
        or set(source) != {"source_sha", "source_clean"}
        or re.fullmatch(r"[0-9a-f]{40}", str(source["source_sha"])) is None
        or source["source_sha"] != context["github_sha"]
        or source["source_clean"] is not True
        or row["source_end"] != source
        or row["state"] != "completed"
    ):
        raise ValueError("checkout evidence is not bound to its exact clean execution")
    for field in ("started_monotonic_ns", "elapsed_ns"):
        if not isinstance(row[field], str) or re.fullmatch(r"[0-9]{1,24}", row[field]) is None:
            raise ValueError("checkout evidence has an invalid clock")
    start, finish = (
        datetime.fromisoformat(row["started_at"]),
        datetime.fromisoformat(row["finished_at"]),
    )
    if start.utcoffset() is None or finish.utcoffset() is None or finish < start:
        raise ValueError("checkout evidence has inconsistent completion timestamps")
    sha = source["source_sha"]
    assert isinstance(sha, str)
    return sha


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("start", "finish"))
    parser.add_argument("--work", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        return record_checkout(args.phase, args.work, args.output)
    except (ValueError, OSError, KeyError, TypeError):
        print("Actions checkout evidence is invalid or incomplete.", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
