"""Observe maintained Make invocations and their actual recipe execution once."""

from __future__ import annotations

import argparse
import os
import platform
import re
import shutil
import sys
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from riverhog_canonical_json import require_canonical_json

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if TYPE_CHECKING:
    from ci_timing import source_identity, write_record
else:
    from scripts.ci_timing import source_identity, write_record

from scripts.validation_evidence import (
    validate_measurement_context,
    validate_phase_costs,
    validation_path_identity,
)
from scripts.validation_process import ValidationInterrupted, run_validation_child
from scripts.validation_resources import validation_storage_budget

ROOT = Path(__file__).resolve().parents[1]
RECIPE_MARKER = "# Maintained target recipes."
SELECTION_VARIABLES = (
    "TESTS",
    "FILES",
    "PYTHON_PATHS",
    "args",
    "PYTEST_ADDOPTS",
    "PYTEST_PLUGINS",
    "UNIT_PYTEST_ARGS",
    "UNIT_SHARD_PYTEST_ARGS",
    "UNIT_REPORT_ARGS",
    "POSTGRES_TESTS",
    "MAKEFILES",
    "RIVERHOG_VALIDATION_SCOPE",
)


def maintained_make_targets(makefile: Path) -> tuple[str, ...]:
    match = re.search(r"^VALIDATION_TARGETS := (.+)$", makefile.read_text(), re.MULTILINE)
    if match is None:
        raise ValueError("maintained Make target declaration is missing")
    values = tuple(match[1].split())
    if not values or len(set(values)) != len(values):
        raise ValueError("maintained Make targets must be nonempty and unique")
    return values


def recipe_plan(makefile: Path, goals: Sequence[str]) -> dict[str, int]:
    """Project the maintained explicit rules; planning never executes Make or recipes."""
    text = makefile.read_text()
    if text.count(RECIPE_MARKER) != 1:
        raise ValueError("the owned Make recipe boundary is missing or ambiguous")
    rules: dict[str, tuple[list[str], int]] = {}
    targets: list[str] = []
    continued = False
    for line in text.split(RECIPE_MARKER, 1)[1].splitlines():
        if line.startswith("\t"):
            if not targets:
                raise ValueError("recipe has no owning target")
            if not continued:
                for target in targets:
                    dependencies, count = rules[target]
                    rules[target] = dependencies, count + 1
            continued = line.rstrip().endswith("\\")
            continue
        continued = False
        if not line or line.startswith(("#", ".PHONY", "ifeq", "ifneq", "else", "endif")):
            continue
        match = re.fullmatch(r"([a-zA-Z0-9_% -]+):(?: (.*))?", line)
        if match is None:
            raise ValueError("unrecognized maintained Make rule syntax")
        targets = match[1].split()
        dependencies = (match[2] or "").split()
        for target in targets:
            if target in rules:
                raise ValueError("multiple owned recipes for one target")
            rules[target] = dependencies, 0
    visited: set[str] = set()
    visiting: set[str] = set()
    plan: dict[str, int] = {}

    def visit(target: str) -> None:
        if target in visiting:
            raise ValueError("cyclic Make recipe dependency")
        if target in visited:
            return
        rule = rules.get(target)
        if rule is None:
            matches = [
                value
                for pattern, value in rules.items()
                if "%" in pattern and re.fullmatch(re.escape(pattern).replace("%", ".+"), target)
            ]
            if len(matches) != 1:
                raise ValueError("requested Make target has no unique owning recipe")
            rule = matches[0]
        dependencies, count = rule
        visiting.add(target)
        for dependency in dependencies:
            visit(dependency)
        visiting.remove(target)
        visited.add(target)
        if count:
            plan[target] = count

    for goal in goals:
        visit(goal)
    if not plan:
        raise ValueError("validation requires executable recipe work")
    return plan


def make_work_definitions(
    goals: Sequence[str], parameters: dict[str, str]
) -> list[dict[str, object]]:
    if TYPE_CHECKING:
        from ci_qualification import validation_work
    else:
        from scripts.ci_qualification import validation_work

    definitions = []
    targets = maintained_make_targets(ROOT / "Makefile")
    for goal in dict.fromkeys(goals):
        if goal not in targets and not (
            "build-%" in targets and re.fullmatch(r"build-[a-zA-Z0-9_-]+", goal)
        ):
            raise ValueError("unknown maintained Make target")
        selector = {
            "unit-shard": ("UNIT_SHARD", "unit-"),
            "image-qualification": ("IMAGE_GROUP", "image-"),
            "compose-shard": ("COMPOSE_LANE", "compose-"),
        }.get(goal)
        name = goal if selector is None else selector[1] + parameters[selector[0]]
        definitions.append(validation_work(name).definition())
    return definitions


def witness_ids(definition: dict[str, object]) -> tuple[str, ...]:
    value = definition["growth_witnesses"]
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError("invalid owner witness bindings")
    return tuple(value)


def owned_execution_spans(timing_root: Path, run_id: str, span: str, source_sha: str) -> set[str]:
    children: dict[str, list[str]] = {}
    descendants: dict[str, Mapping[str, object]] = {}
    for path in (timing_root / "make" / run_id).glob("*/manifest.json"):
        row = require_canonical_json(path.read_bytes())
        if not isinstance(row, dict) or row.get("run_id") != run_id:
            raise ValueError("invalid descendant execution manifest")
        identity = row.get("span")
        if not isinstance(identity, str):
            raise ValueError("missing descendant execution identity")
        if identity == span or row.get("qualification_eligible") is not True:
            continue
        if identity in descendants:
            raise ValueError("duplicate descendant execution identity")
        descendants[identity] = row
        parent = row.get("parent_span")
        if isinstance(parent, str):
            children.setdefault(parent, []).append(identity)
    visited: set[str] = set()
    pending = [span]
    while pending:
        current = pending.pop()
        if current in visited:
            raise ValueError("duplicate or cyclic execution ancestry")
        if current != span:
            descendant = descendants[current]
            if (
                descendant.get("source_sha") != source_sha
                or descendant.get("source_clean") is not True
                or descendant.get("source_end") != {"source_sha": source_sha, "source_clean": True}
            ):
                raise ValueError("descendant execution is not bound to the tested source")
        visited.add(current)
        pending.extend(children.get(current, ()))
    return visited


def retain_completed_runs(timing_root: Path, current: str) -> None:
    keep = int(os.environ.get("RIVERHOG_VALIDATION_RETAIN_RUNS", "32"))
    budget = validation_storage_budget()
    if min(keep, budget) < 1:
        raise ValueError("validation retention resources must be positive")
    completed: list[tuple[float, Path]] = []
    for group in (timing_root / "make").glob("*"):
        if (
            not group.is_dir()
            or group.is_symlink()
            or group.name == current
            or list(group.glob(".active-*"))
        ):
            continue
        manifests = list(group.glob("*/manifest.json"))
        if not manifests:
            continue
        rows = []
        try:
            for path in manifests:
                row = require_canonical_json(path.read_bytes())
                if (
                    not isinstance(row, dict)
                    or row.get("format") != "riverhog-make-validation/v1"
                    or row.get("run_id") != group.name
                    or row.get("state")
                    not in {"completed", "interrupted", "launch-failed", "evidence-failed"}
                ):
                    break
                rows.append(row)
        except (OSError, ValueError):
            continue
        if len(rows) == len(manifests):
            completed.append((max(path.stat().st_mtime for path in manifests), group))
    completed.sort()

    def storage_bytes() -> int:
        return sum(
            path.stat().st_size
            for path in timing_root.rglob("*")
            if path.is_file() and not path.is_symlink()
        )

    size = storage_bytes()
    while completed and (len(completed) >= keep or size >= budget):
        _, group = completed.pop(0)
        run = group.name
        shutil.rmtree(group)
        for lane in (timing_root / "pytest").glob("*"):
            owned = lane / run
            if owned.is_dir() and not owned.is_symlink():
                shutil.rmtree(owned)
        # Shell phase streams and their scoped diagnostics share the same owned run.
        for lane in timing_root.iterdir():
            if not lane.is_dir() or lane.is_symlink() or lane.name in {"make", "pytest"}:
                continue
            owned = lane / run
            phase = owned / "phases.jsonl"
            if (
                not owned.is_dir()
                or owned.is_symlink()
                or not phase.is_file()
                or phase.is_symlink()
            ):
                continue
            try:
                valid, seen = True, False
                with phase.open("rb") as stream:
                    while line := stream.readline(1024 * 1024 + 1):
                        if len(line) > 1024 * 1024:
                            valid = False
                            break
                        row = require_canonical_json(line.rstrip(b"\n"))
                        if (
                            not isinstance(row, dict)
                            or row.get("format") != "riverhog-ci-phase-timing/v1"
                            or row.get("run_id") != run
                        ):
                            valid = False
                            break
                        seen = True
                if valid and seen:
                    shutil.rmtree(owned)
            except (OSError, ValueError):
                continue
        size = storage_bytes()
    if size >= budget:
        raise ValueError(
            "validation storage budget is exhausted by retained active or incomplete evidence"
        )


def observe_make(makefile: Path, goals: Sequence[str]) -> int:
    if not goals or any(re.fullmatch(r"[a-zA-Z0-9_-]+", goal) is None for goal in goals):
        raise ValueError("invalid Make goal")
    started = time.monotonic()
    root = Path.cwd()
    source = source_identity(root)
    run_id = validation_path_identity(os.environ.get("RIVERHOG_CI_RUN_ID", uuid4().hex))
    span = uuid4().hex
    timing_root = Path(os.environ.get("RIVERHOG_CI_TIMING_DIR", str(root / "build/ci-timing")))
    retain_completed_runs(timing_root, run_id)
    directory = timing_root / "make" / run_id / span
    expected = recipe_plan(makefile, goals)
    selectors = [name for name in SELECTION_VARIABLES if os.environ.get(name)]
    parameters = {
        name: os.environ.get(name, "")
        for name, goal in (
            ("UNIT_SHARD", "unit-shard"),
            ("IMAGE_GROUP", "image-qualification"),
            ("COMPOSE_LANE", "compose-shard"),
        )
        if goal in goals
    }
    try:
        definitions = make_work_definitions(goals, parameters)
    except (ValueError, KeyError):
        definitions = []
        parameters = {name: "unverified" for name in parameters}
    record: dict[str, object] = {
        "format": "riverhog-make-validation/v1",
        **source,
        "run_id": run_id,
        "span": span,
        "parent_span": os.environ.get("RIVERHOG_VALIDATION_PARENT_SPAN"),
        "goals": list(goals),
        "expected_recipes": expected,
        "work_definitions": definitions,
        "parameters": parameters,
        "selection_overrides": selectors,
        "observed_conditions": {
            "platform": sys.platform,
            "architecture": platform.machine(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "cpu_affinity_count": len(os.sched_getaffinity(0))
            if hasattr(os, "sched_getaffinity")
            else None,
        },
        "qualification_eligible": root.resolve() == ROOT
        and makefile.resolve() == ROOT / "Makefile"
        and not selectors
        and bool(definitions)
        and source["source_clean"] is True,
        "started_at": datetime.now(UTC).isoformat(),
        "state": "running",
    }
    output = directory / "manifest.json"
    write_record(output, record)
    shell = directory / "bash"
    shell.write_text(
        "#!/usr/bin/env bash\n"
        'exec "$RIVERHOG_VALIDATION_MAKE_PYTHON" "$RIVERHOG_VALIDATION_MAKE_SCRIPT" '
        'recipe --target "$RIVERHOG_VALIDATION_RECIPE_TARGET" -- "$@"\n'
    )
    shell.chmod(0o700)
    env = {
        **os.environ,
        "RIVERHOG_VALIDATION_MAKE_REENTRY": " ".join(goals),
        "RIVERHOG_VALIDATION_MAKE_PYTHON": sys.executable,
        "RIVERHOG_VALIDATION_MAKE_SCRIPT": str(Path(__file__).resolve()),
        "RIVERHOG_VALIDATION_MAKE_SHELL": str(shell),
        "RIVERHOG_VALIDATION_MAKE_DIRECTORY": str(directory),
        "RIVERHOG_VALIDATION_MAKE_SPAN": span,
        "RIVERHOG_CI_RUN_ID": run_id,
    }
    execution_started = time.monotonic()
    state, status = "completed", 2
    try:
        status = run_validation_child(["make", "-f", str(makefile), *goals], cwd=root, env=env)
    except OSError:
        state = "launch-failed"
    except ValidationInterrupted as interrupted:
        state, status = "interrupted", 128 + interrupted.signum
    finally:
        execution_finished = time.monotonic()
        recipes = []
        growth: dict[str, object] = {}
        try:
            for path in sorted((directory / "recipes").glob("*.json")):
                recipes.append(require_canonical_json(path.read_bytes()))
            if state == "completed" and record["qualification_eligible"] and source["source_clean"]:
                from scripts.validation_witnesses import collect_test_witnesses

                spans = owned_execution_spans(timing_root, run_id, span, str(source["source_sha"]))
                paths = []
                for path in (timing_root / "pytest").glob(f"*/{run_id}/*/*.json"):
                    row = require_canonical_json(path.read_bytes())
                    if isinstance(row, dict) and row.get("execution_span") in spans:
                        paths.append(path)
                for definition in definitions:
                    growth[str(definition["name"])] = (
                        collect_test_witnesses(
                            paths,
                            root=root,
                            expected=witness_ids(definition),
                            source_sha=str(source["source_sha"]),
                            run_id=run_id,
                        )
                        if definition["growth_witnesses"]
                        else {}
                    )
        except (ValueError, OSError, TypeError, KeyError):
            state = "evidence-failed"
        after = source_identity(root)
        finished = time.monotonic()
        record.update(
            state=state,
            exit_status=status,
            source_end=after,
            finished_at=datetime.now(UTC).isoformat(),
            elapsed_seconds=finished - started,
            phase_costs={
                "preparation": execution_started - started,
                "command-including-owner-cleanup": execution_finished - execution_started,
                "evidence-collection": finished - execution_finished,
            },
            recipes=recipes,
            growth_results=growth,
        )
        write_record(output, record)
    try:
        report = validate_make_receipt(
            record,
            makefile=makefile,
            goals=goals,
            parameters=parameters,
            source=source,
            run_id=run_id,
        )
        if report["qualification_exit_status"] and not status:
            status = 2
    except (ValueError, OSError, TypeError, KeyError):
        status = status or 2
    print(f"Make validation: {finished - started:.3f}s; receipt {output}", file=sys.stderr)
    return status


def validate_make_receipt(
    record: object,
    *,
    makefile: Path,
    goals: Sequence[str],
    parameters: dict[str, str],
    source: dict[str, object],
    run_id: str,
    require_qualification: bool = False,
) -> dict[str, object]:
    if not isinstance(record, dict) or record.get("format") != "riverhog-make-validation/v1":
        raise ValueError("invalid Make receipt")
    fields = {
        "format",
        "source_sha",
        "source_clean",
        "run_id",
        "span",
        "parent_span",
        "goals",
        "expected_recipes",
        "work_definitions",
        "parameters",
        "selection_overrides",
        "observed_conditions",
        "qualification_eligible",
        "started_at",
        "state",
        "exit_status",
        "source_end",
        "finished_at",
        "elapsed_seconds",
        "phase_costs",
        "recipes",
        "growth_results",
    }
    if set(record) != fields or type(record["qualification_eligible"]) is not bool:
        raise ValueError("missing or unexpected Make receipt fields")
    conditions = record["observed_conditions"]
    if not isinstance(conditions, dict) or set(conditions) != {
        "platform",
        "architecture",
        "python",
        "cpu_count",
        "cpu_affinity_count",
    }:
        raise ValueError("invalid observed Make conditions")
    if any(
        not isinstance(conditions[key], str) or not conditions[key]
        for key in ("platform", "architecture", "python")
    ):
        raise ValueError("missing observed runtime conditions")
    if any(
        conditions[key] is not None and (type(conditions[key]) is not int or conditions[key] < 1)
        for key in ("cpu_count", "cpu_affinity_count")
    ):
        raise ValueError("invalid observed processor conditions")
    selectors = record["selection_overrides"]
    if (
        not isinstance(selectors, list)
        or len(set(selectors)) != len(selectors)
        or set(selectors) - set(SELECTION_VARIABLES)
    ):
        raise ValueError("invalid Make selection observations")
    if record["qualification_eligible"] and (selectors or not record["work_definitions"]):
        raise ValueError("iteration work cannot claim full qualification eligibility")
    if record.get("run_id") != run_id or record.get("goals") != list(goals):
        raise ValueError("Make receipt work or run differs")
    if (
        any(record.get(key) != value for key, value in source.items())
        or record.get("source_end") != source
    ):
        raise ValueError("Make receipt source changed")
    validation_path_identity(run_id)
    if record.get("parameters") != parameters:
        raise ValueError("Make entrypoint selectors differ")
    try:
        definitions = make_work_definitions(goals, parameters)
    except (KeyError, ValueError):
        definitions = []
    if record.get("work_definitions") != definitions:
        raise ValueError("Make work differs from maintained definitions")
    validate_phase_costs(record)
    if record.get("expected_recipes") != recipe_plan(makefile, goals):
        raise ValueError("Make recipe coverage differs from source")
    if record.get("state") != "completed" or type(record.get("exit_status")) is not int:
        raise ValueError("Make execution is incomplete")
    recipes = record.get("recipes")
    if not isinstance(recipes, list):
        raise ValueError("Make recipe evidence is absent")
    counts: Counter[str] = Counter()
    failed = False
    seen: set[str] = set()
    for recipe in recipes:
        if not isinstance(recipe, dict) or recipe.get("format") != "riverhog-make-recipe/v1":
            raise ValueError("invalid recipe evidence")
        if set(recipe) != {
            "format",
            "run_id",
            "span",
            "id",
            "target",
            "started_at",
            "state",
            "exit_status",
            "elapsed_seconds",
            "finished_at",
        }:
            raise ValueError("missing or unexpected recipe fields")
        if recipe.get("run_id") != run_id or recipe.get("span") != record["span"]:
            raise ValueError("recipe belongs to another invocation")
        identity = recipe.get("id")
        if not isinstance(identity, str) or identity in seen:
            raise ValueError("duplicate recipe evidence")
        validation_path_identity(identity)
        seen.add(identity)
        validate_measurement_context(recipe)
        target = recipe.get("target")
        if not isinstance(target, str) or target not in record["expected_recipes"]:
            raise ValueError("unexpected recipe")
        counts[target] += 1
        if recipe.get("state") != "completed" or type(recipe.get("exit_status")) is not int:
            raise ValueError("recipe execution is incomplete")
        failed = failed or recipe["exit_status"] != 0
    if dict(counts) != record["expected_recipes"]:
        raise ValueError("missing recipe execution evidence")
    eligible = record.get("qualification_eligible") is True and source.get("source_clean") is True
    if require_qualification and not eligible:
        raise ValueError("iteration observations cannot establish required qualification")
    growth = record.get("growth_results")
    if not isinstance(growth, dict):
        raise ValueError("missing owner growth results")
    if eligible:
        expected_growth = {str(item["name"]): set(witness_ids(item)) for item in definitions}
        if set(growth) != set(expected_growth):
            raise ValueError("missing required owner growth coverage")
        for name, outcomes in growth.items():
            if (
                not isinstance(outcomes, dict)
                or set(outcomes) != expected_growth[name]
                or any(value not in ("pass", "fail") for value in outcomes.values())
            ):
                raise ValueError("missing or invalid required owner growth evidence")
            failed = failed or any(value == "fail" for value in outcomes.values())
    return {
        "evidence_complete": True,
        "qualification_exit_status": 1 if record["exit_status"] or failed else 0,
        "qualification_eligible": eligible,
    }


def observe_recipe(target: str, shell_arguments: Sequence[str]) -> int:
    directory = Path(os.environ["RIVERHOG_VALIDATION_MAKE_DIRECTORY"])
    identity = uuid4().hex
    record: dict[str, object] = {
        "format": "riverhog-make-recipe/v1",
        "run_id": validation_path_identity(os.environ["RIVERHOG_CI_RUN_ID"]),
        "span": validation_path_identity(os.environ["RIVERHOG_VALIDATION_MAKE_SPAN"]),
        "id": identity,
        "target": target,
        "started_at": datetime.now(UTC).isoformat(),
        "state": "running",
    }
    output = directory / "recipes" / (identity + ".json")
    write_record(output, record)
    started = time.monotonic()
    state, status = "completed", 2
    env = dict(os.environ)
    for name in (
        "RIVERHOG_VALIDATION_MAKE_REENTRY",
        "RIVERHOG_VALIDATION_MAKE_PYTHON",
        "RIVERHOG_VALIDATION_MAKE_SCRIPT",
        "RIVERHOG_VALIDATION_MAKE_DIRECTORY",
        "RIVERHOG_VALIDATION_MAKE_SPAN",
        "RIVERHOG_VALIDATION_MAKE_SHELL",
        "RIVERHOG_VALIDATION_RECIPE_TARGET",
    ):
        env.pop(name, None)
    env["RIVERHOG_VALIDATION_PARENT_SPAN"] = str(record["span"])
    env["RIVERHOG_VALIDATION_ACTIVE_SPAN"] = str(record["span"])
    try:
        status = run_validation_child(["bash", *shell_arguments], cwd=Path.cwd(), env=env)
    except OSError:
        state = "launch-failed"
    except ValidationInterrupted as interrupted:
        state, status = "interrupted", 128 + interrupted.signum
    finally:
        record.update(
            state=state,
            exit_status=status,
            elapsed_seconds=time.monotonic() - started,
            finished_at=datetime.now(UTC).isoformat(),
        )
        write_record(output, record)
    return status


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    observe = commands.add_parser("observe")
    observe.add_argument("--makefile", type=Path, required=True)
    observe.add_argument("--goal", action="append", required=True)
    recipe = commands.add_parser("recipe")
    recipe.add_argument("--target", required=True)
    recipe.add_argument("shell_arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    try:
        if args.command == "observe":
            return observe_make(args.makefile, args.goal)
        arguments = (
            args.shell_arguments[1:] if args.shell_arguments[:1] == ["--"] else args.shell_arguments
        )
        return observe_recipe(args.target, arguments)
    except (ValueError, OSError, KeyError, TypeError):
        print("Make validation evidence is invalid or incomplete", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
