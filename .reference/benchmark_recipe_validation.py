"""Measure retained static-definition loads, not end-to-end scale qualification.

Run from a locked repository workspace with:
  mise x -- uv run --locked --all-packages --group dev \
    python .reference/benchmark_recipe_validation.py --output /tmp/recipe-loads.json
Use the same script/environment on the baseline and candidate for comparison.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from sqlalchemy import select
from stove0_core import recipe_definitions
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_recipe_config import load_recipe_catalog


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("iterations must be positive")
    root = args.repo.resolve()
    catalog = load_recipe_catalog(root / "qualification/fixtures/stove0/recipes.yaml")
    report = {
        "measurement": "source-native retained recipe load; no Docker or media execution",
        "python": platform.python_version(),
        "source_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip(),
        "tracked_changes": bool(subprocess.check_output(["git", "diff", "HEAD"], cwd=root)),
        "iterations": args.iterations,
        "recipes": {},
    }
    with TemporaryDirectory() as directory:
        state = SqlAlchemyStateStore(f"sqlite:///{directory}/control.db")
        try:
            for name in ("stove0.audio-archive/v1", "stove0.conformance-media/v1"):
                recipe = catalog.recipe(name)
                state.recipe_definitions.retain_tree(recipe, catalog.closure)
                with state.engine.connect() as connection:
                    table = state.recipe_definitions.table
                    row = connection.execute(
                        select(table).where(table.c.recipe_sha256 == recipe.sha256)
                    ).mappings().one()
                cache = getattr(recipe_definitions, "_verified_documents", None)
                if cache is not None:
                    cache.cache_clear()
                started = perf_counter()
                cold = state.recipe_definitions.load(recipe.ref)
                cold_seconds = perf_counter() - started
                samples = []
                for index in range(args.iterations):
                    # Recreated stores are normal inside planning scopes. A
                    # per-store cache would not help this real access pattern.
                    scoped = state.planning_context("work", f"{index:064x}")
                    started = perf_counter()
                    loaded = scoped.recipe_definitions.load(recipe.ref)
                    samples.append(perf_counter() - started)
                    assert loaded == cold
                report["recipes"][name] = {
                    "recipe_sha256": recipe.sha256,
                    "document_bytes": sum(
                        len(row[key].encode("utf-8")) for key in ("recipe_json", "closure_json")
                    ),
                    "cold_seconds": cold_seconds,
                    "warm_seconds": samples,
                    "warm_median_seconds": statistics.median(samples),
                    "cache_info": cache.cache_info()._asdict() if cache is not None else None,
                }
        finally:
            state.engine.dispose()
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
