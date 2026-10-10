"""Pre-decision source-anchored growth probe; not a Riverhog qualification.

Uses only the Python standard library. Run from the reference branch with:
  python .reference/probe_scaling.py --source-root . --output /tmp/scale-probe.json

Source methods are AST-checked before the one self-contained lookup method is
executed; neither a live service nor a simulated archive completion is claimed.
"""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

PUBLICATION = Path("some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/publication.py")
RELEVANCE = Path("riverhog/src/riverhog_core/canonical_discovery_relevance.py")


def checked_lookup(source: Path):
    tree = ast.parse(source.read_text(encoding="utf-8"))
    cls = next((node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "MediaPublicationPlan"), None)
    if cls is None:
        raise AssertionError("MediaPublicationPlan is absent; reconcile research with current source")
    method = next((node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "decision_for"), None)
    if method is None or len(method.body) != 2:
        raise AssertionError("decision_for changed shape; re-audit before interpreting counts")
    loop, fallback = method.body
    if not (isinstance(loop, ast.For) and isinstance(loop.iter, ast.Attribute) and loop.iter.attr == "decisions"
            and isinstance(loop.iter.value, ast.Name) and loop.iter.value.id == "self"
            and isinstance(fallback, ast.Raise)):
        raise AssertionError("decision lookup is no longer the audited linear scan")
    if len(loop.body) != 1 or not isinstance(loop.body[0], ast.If) or len(loop.body[0].body) != 1:
        raise AssertionError("decision lookup changed; inspect updated source")
    match = loop.body[0].test
    if not (isinstance(match, ast.Compare) and isinstance(match.left, ast.Attribute)
            and match.left.attr == "output_id" and len(match.ops) == 1
            and isinstance(match.ops[0], ast.Eq) and isinstance(match.comparators[0], ast.Name)
            and match.comparators[0].id == "output_id"):
        raise AssertionError("decision comparison changed; inspect source")
    # Execute only this reviewed two-statement method, without importing Riverhog
    # or providing its runtime dependencies. Annotations are nonessential here.
    method = ast.FunctionDef(name=method.name, args=method.args, body=method.body,
                             decorator_list=[], returns=None, type_comment=None)
    for parameter in [*method.args.posonlyargs, *method.args.args, *method.args.kwonlyargs]:
        parameter.annotation = None
    namespace: dict[str, object] = {"ValueError": ValueError}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[method], type_ignores=[])), str(source), "exec"), namespace)
    return namespace["decision_for"]


def check_recorded_history_expansion(source: Path) -> None:
    tree = ast.parse(source.read_text(encoding="utf-8"))
    function = next((node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "member_relevance"), None)
    if function is None:
        raise AssertionError("member_relevance moved; re-audit scope semantics")
    traversals = []
    for node in ast.walk(function):
        if not isinstance(node, ast.For) or not isinstance(node.iter, ast.Call):
            continue
        if (ast.unparse(node.iter) == "closure.snapshots()" and
            any(isinstance(inner, ast.For) and "_assertions(snapshot)" == ast.unparse(inner.iter)
                for inner in node.body)):
            traversals.append(node)
    if len(traversals) != 1:
        raise AssertionError("recorded-history expansion has changed; re-audit")
    if not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
               and node.func.id == "mark" and len(node.args) == 3
               and isinstance(node.args[2], ast.Constant)
               and node.args[2].value == "recorded-history"
               for node in ast.walk(traversals[0])):
        raise AssertionError("recorded-history attribution rule changed")


@dataclass
class ComparisonCounter:
    comparisons: int = 0


class Decision:
    def __init__(self, label: str, counter: ComparisonCounter):
        self.label, self.counter = label, counter

    @property
    def output_id(self) -> str:
        self.counter.comparisons += 1
        return self.label


def probe(count: int, method) -> dict[str, object]:
    if count < 1:
        raise ValueError("positive cardinality required")
    outputs = 2 * count + 1  # One media output plus generated XMP per WAV, one retained XMP.
    counter = ComparisonCounter()
    decisions = tuple(Decision(f"out-{index:08d}", counter) for index in range(outputs))
    instance = SimpleNamespace(decisions=decisions)
    baseline_start = time.perf_counter()
    for d in decisions:
        assert method(instance, d.label) is d
    baseline_seconds = time.perf_counter() - baseline_start
    expected = outputs * (outputs + 1) // 2
    assert counter.comparisons == expected, (counter.comparisons, expected)
    try:
        method(instance, "unknown-member")
    except ValueError:
        pass
    else:
        raise AssertionError("unknown output must fail closed")
    # An illustrative indexed representation; NOT a proposed product patch.
    indexed_start = time.perf_counter()
    lookup = {d.label: d for d in decisions}
    for d in decisions:
        assert lookup[d.label] is d
    indexed_seconds = time.perf_counter() - indexed_start
    return {
        "input_files": count,
        "declared_output_decisions": outputs,
        "executed_lookup_comparisons": expected,
        "one_pass_linear_lookup_seconds": round(baseline_seconds, 6),
        "illustrative_index_build_and_lookup_seconds": round(indexed_seconds, 6),
        "status": "source-method-exercised, not full-target-or-docker",
    }


def model(n: int) -> dict[str, int]:
    m = 2 * n + 1
    return {
        "input_files": n,
        "output_decisions": m,
        "comparisons_for_one_lookup_each_output": m * (m + 1) // 2,
        "incidence_links_if_32_selected_shared_assertions_per_member": n * 32,
        "incidence_links_if_n_selected_shared_assertions_per_member": n * n,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path("."))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = args.source_root.resolve()
    method = checked_lookup(root / PUBLICATION)
    check_recorded_history_expansion(root / RELEVANCE)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    values = {
        "format": "riverhog-external-scaling-source-probe/v1",
        "source_revision": revision,
        "scope": "AST-matched source lookup and purely illustrative incidence models",
        "observed_native_source_method": [probe(n, method) for n in (128, 512, 1000, 2048)],
        "analytical_scenarios_not_measured": [model(n) for n in (128, 512, 1000, 10000, 100000)],
        "proof_limits": [
            "timings depend on Python host; comparisons are deterministically counted",
            "the incidence models do not measure actual Riverhog relevance semantics or PostgreSQL",
            "no 10000 or 100000 member runtime was run",
            "decision_for is only one part of the full accepted target and archive lifecycle",
            "no deployed images, PostgreSQL, storage provider, CI, or final end-to-end qualification",
        ],
    }
    document = json.dumps(values, sort_keys=True, indent=2) + "\n"
    if args.output is not None:
        args.output.write_text(document, encoding="utf-8")
    else:
        sys.stdout.write(document)


if __name__ == "__main__":
    main()
