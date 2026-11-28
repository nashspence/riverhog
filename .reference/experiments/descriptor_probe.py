#!/usr/bin/env python3
"""Finite plaintext work-count probe for the actual independent recovery reader.

No encrypted timing, product speedup, complete semantic recovery or budget pass
is established. Fixtures are imported from the maintained proposal test module.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sqlite3
import sys
import tempfile
import types
from collections import Counter
from contextlib import closing, contextmanager, nullcontext
from pathlib import Path
from unittest.mock import patch


def load_file(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_reader(name: str, path: Path):
    package = types.ModuleType(name)
    package.__path__ = [str(path.parent)]
    sys.modules[name] = package
    return load_file(name + "._provenance_reader", path)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class SqlCounts:
    def __init__(self):
        self.phase = "prepare"
        self.connections = []
        self.explaining = False

    @contextmanager
    def active(self):
        owner = self
        original = sqlite3.connect

        class Cursor(sqlite3.Cursor):
            def execute(self, statement, parameters=(), /):
                self.connection.probe_record["statements"][owner.phase] += 1
                if statement.lstrip().upper().startswith("SELECT"):
                    owner.explaining = True
                    try:
                        raw = sqlite3.Connection.execute(
                            self.connection, "EXPLAIN QUERY PLAN " + statement, parameters
                        )
                        try:
                            plan = tuple(str(row[3]) for row in raw.fetchall())
                        finally:
                            raw.close()
                        self.connection.probe_record["plans"].add((statement, plan))
                    finally:
                        owner.explaining = False
                return super().execute(statement, parameters)

            def executescript(self, statement, /):
                self.connection.probe_record["statements"][owner.phase] += 1
                return super().executescript(statement)

            def _count(self, n):
                self.connection.probe_record["rows"][owner.phase] += n

            def fetchone(self):
                row = super().fetchone()
                self._count(int(row is not None))
                return row

            def fetchmany(self, size=None):
                rows = super().fetchmany() if size is None else super().fetchmany(size)
                self._count(len(rows))
                return rows

            def fetchall(self):
                rows = super().fetchall()
                self._count(len(rows))
                return rows

            def __next__(self):
                row = super().__next__()
                self._count(1)
                return row

            def close(self):
                super().close()
                self.connection.probe_record["cursor_closes"] += 1

        class Connection(sqlite3.Connection):
            def cursor(self, *args, **kwargs):
                kwargs.setdefault("factory", Cursor)
                cursor = super().cursor(*args, **kwargs)
                self.probe_record["cursors"] += 1
                return cursor

            def execute(self, statement, parameters=(), /):
                return self.cursor().execute(statement, parameters)

            def executescript(self, statement, /):
                return self.cursor().executescript(statement)

            def close(self):
                super().close()
                self.probe_record["closed"] = True

        def connect(*args, **kwargs):
            if kwargs.get("factory", sqlite3.Connection) is not sqlite3.Connection:
                raise RuntimeError("probe does not replace an existing connection factory")
            kwargs["factory"] = Connection
            connection = original(*args, **kwargs)
            record = {
                "database": Path(str(args[0])).name,
                "vm_steps": Counter(),
                "rows": Counter(),
                "statements": Counter(),
                "plans": set(),
                "cursors": 0,
                "cursor_closes": 0,
                "closed": False,
            }
            connection.probe_record = record
            owner.connections.append(record)

            def progress():
                if not owner.explaining:
                    record["vm_steps"][owner.phase] += 1
                return 0

            connection.set_progress_handler(progress, 1)
            return connection

        with patch.object(sqlite3, "connect", connect):
            yield self

    def result(self):
        result = []
        for row in self.connections:
            result.append({
                **row,
                "plans": [
                    {"sql": sql, "details": list(details)}
                    for sql, details in sorted(row["plans"])
                ],
            })
        return result


def source_counts(source, case):
    categories = {"provenance/root.json.age": "root"}
    for document in case.descriptors:
        categories[document.metadata_path] = "descriptor"
        payload = getattr(document, "payload", None)
        if payload is not None:
            categories[payload.path] = payload.kind + "_payload"
    for path in case.structure_paths:
        categories[path] = "structure"
    opens, closed, returned_bytes = Counter(), Counter(), Counter()
    for path, count in source.opens.items():
        category = categories[path]
        opens[category] += count
        returned_bytes[category] += count * len(case.objects[path])
    for path, count in source.closed.items():
        closed[categories[path]] += count
    return {
        "opens": dict(opens),
        "completed_or_closed_streams": dict(closed),
        "fully_consumed_plaintext_bytes": dict(returned_bytes),
    }


def difference(before, after):
    return {
        category: {
            key: after[category].get(key, 0) - before[category].get(key, 0)
            for key in sorted(before[category].keys() | after[category].keys())
        }
        for category in after
    }


def attach_descriptor_counter(reader, counter, sql):
    original = getattr(reader, "_descriptors", None)
    if original is None or getattr(reader, "_probe_counted", False):
        return False

    def descriptors(*args, **kwargs):
        for document in original(*args, **kwargs):
            counter[sql.phase] += 1
            yield document

    try:
        reader._descriptors = descriptors
        reader._probe_counted = True
    except (AttributeError, TypeError):
        return False
    return True


def measure(mode, module, fixture, dimensions, repeat_count):
    case_dimensions = dict(dimensions)
    selected_structures = case_dimensions.pop("selected_structures", None)
    case = fixture.make_archive_case(**case_dimensions)
    selected_paths = case.structure_paths[:selected_structures]
    source = fixture.PlaintextSource(case.objects)
    reader = module.CanonicalProvenanceArchiveReader(
        source,
        expected_root_sha256=case.root.identity,
        archive_generation=case.root.archive_generation,
        artifact_set_sha256=case.root.artifact_set_sha256,
    )
    sql = SqlCounts()
    descriptor_yields = Counter()
    attached = attach_descriptor_counter(reader, descriptor_yields, sql)
    phases = []
    expected_headers = [
        (journal_id, len(data), hashlib.sha256(data).hexdigest())
        for journal_id, data in case.journals.items()
    ]
    with tempfile.TemporaryDirectory(prefix="recovery-descriptor-probe-") as temporary:
        before = source_counts(source, case)
        context = (
            reader.prepare(workspace=Path(temporary))
            if mode == "prepared"
            else nullcontext(reader)
        )
        with sql.active():
            with context as view:
                attached = attach_descriptor_counter(view, descriptor_yields, sql) or attached
                after = source_counts(source, case)
                phases.append({"phase": "prepare", **difference(before, after)})
                for repeat in range(repeat_count):
                    def run_phase(name, action):
                        sql.phase = f"{repeat}:{name}"
                        first = source_counts(source, case)
                        action()
                        last = source_counts(source, case)
                        phases.append({"phase": sql.phase, **difference(first, last)})

                    def headers_and_ranges():
                        observed = []
                        for header in view.iter_journal_headers():
                            observed.append(header)
                            data = b"".join(view.iter_journal_range(header[0]))
                            assert data == case.journals[header[0]]
                        assert observed == expected_headers

                    def metadata():
                        for journal_id, size, digest in expected_headers:
                            assert view.journal_metadata(journal_id) == (size, digest)

                    def bindings():
                        assert list(view.iter_bindings()) == [
                            binding.to_mapping() for binding in case.bindings
                        ]

                    def structures():
                        for path in selected_paths:
                            assert b"".join(view.read_structure(path)) == case.objects[path]

                    run_phase("headers_ranges", headers_and_ranges)
                    run_phase("journal_metadata", metadata)
                    run_phase("bindings", bindings)
                    run_phase("structures", structures)
                sql.phase = "teardown"
        totals = source_counts(source, case)
        assert totals["opens"] == totals["completed_or_closed_streams"]
        assert not list(Path(temporary).iterdir()), "prepared scratch remains after scope"
    d = len(case.descriptors)
    j = len(case.journals)
    if mode == "baseline":
        first = next(x for x in phases if x["phase"] == "0:headers_ranges")
        assert first["opens"]["descriptor"] == 2 * (j + 1) * d
        assert first["opens"]["root"] == j + 1
    else:
        assert totals["opens"].get("descriptor", 0) == d
        assert totals["opens"].get("root", 0) == 1
        assert all(row["closed"] for row in sql.connections)
    return {
        "mode": mode,
        "dimensions": {
            **dimensions,
            "D_including_terminal": d,
            "binding_pages": sum(
                getattr(getattr(x, "payload", None), "kind", None) == "bindings"
                for x in case.descriptors
            ),
            "structure_objects": len(case.structure_paths),
            "selected_structure_objects": len(selected_paths),
        },
        "repeats_in_same_owner": repeat_count,
        "phases": phases,
        "totals": totals,
        "descriptor_yields": dict(descriptor_yields) if attached else None,
        "sqlite": sql.result(),
        "verified_exact_headers_journals_bindings_structure": True,
        "source_streams_received": True,
        "scratch_removed": True,
    }


def measure_stage(mode, module, fixture, dimensions):
    """Actual staging, neutral journal/corpus validation, then complete bindings.

    This is not recover_collection: payload staging, mapping, selected history,
    metadata-head admission and durable publication are outside this boundary.
    """
    case = fixture.make_archive_case(**dimensions, semantic_journals=True)
    source = fixture.PlaintextSource(case.objects)
    sql = SqlCounts()
    phases = []
    with tempfile.TemporaryDirectory(prefix="recovery-stage-probe-") as temporary:
        staging, scratch = Path(temporary) / "output", Path(temporary) / "scratch"
        staging.mkdir()
        scratch.mkdir()
        before = source_counts(source, case)
        sql.phase = "stage_provenance"

        @contextmanager
        def run_stage():
            if mode == "prepared":
                with module._stage_provenance(
                    fixture._selection(case, source), staging=staging, scratch=scratch
                ) as result:
                    yield result
            else:
                result = module._stage_provenance(
                    fixture._selection(case, source), staging=staging, scratch=scratch
                )
                with closing(result[1]):
                    yield result

        with sql.active():
            with run_stage() as (view, journals, count):
                after_stage = source_counts(source, case)
                phases.append({"phase": sql.phase, **difference(before, after_stage)})
                assert count == len(case.journals)
                with closing(journals.execute(
                    "SELECT journal_id, bytes, sha256, path FROM journals ORDER BY journal_id"
                )) as rows:
                    observed = list(rows)
                assert len(observed) == count
                for journal_id, size, digest, path in observed:
                    raw = case.journals[journal_id]
                    assert (size, digest, Path(path).read_bytes()) == (
                        len(raw), hashlib.sha256(raw).hexdigest(), raw
                    )
                assert (staging / "structure/provenance-root.json").read_bytes() == (
                    case.objects["provenance/root.json.age"]
                )
                sql.phase = "follow_on_bindings"
                assert list(view.iter_bindings()) == [x.to_mapping() for x in case.bindings]
                phases.append({
                    "phase": sql.phase,
                    **difference(after_stage, source_counts(source, case)),
                })
                sql.phase = "teardown"
        totals = source_counts(source, case)
        assert totals["opens"] == totals["completed_or_closed_streams"]
        assert all(row["closed"] for row in sql.connections)
        assert not list(scratch.glob("provenance-metadata-*"))
        j, d = len(case.journals), len(case.descriptors)
        if mode == "baseline":
            assert phases[0]["opens"]["descriptor"] == (2 * j + 3) * d
            assert phases[0]["opens"]["root"] == j + 3
            assert totals["opens"]["descriptor"] == (2 * j + 6) * d
            assert totals["opens"]["root"] == j + 4
        else:
            assert totals["opens"]["descriptor"] == d
            assert totals["opens"]["root"] == 1
    return {
        "mode": mode,
        "boundary": "actual _stage_provenance plus complete iter_bindings; not full mapping/recovery",
        "dimensions": {
            **dimensions,
            "D_including_terminal": len(case.descriptors),
        },
        "phases": phases,
        "totals": totals,
        "sqlite": sql.result(),
        "verified_exact_staged_root_journals_and_binding_values": True,
        "actual_neutral_journal_and_corpus_validators": True,
        "source_streams_received": True,
        "metadata_index_removed": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--baseline-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=("baseline", "prepared", "both"), default="both")
    parser.add_argument("--sizes", nargs="+", type=int, default=[1, 2, 4, 8, 16])
    parser.add_argument(
        "--axes", nargs="+", default=["journals", "segments", "bindings", "selected_structures"],
        choices=("journals", "segments", "bindings", "selected_structures"),
    )
    parser.add_argument("--include-stage", action="store_true")
    parser.add_argument("--repeats", type=int, choices=(1, 2), default=2)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("--output must be a new file")
    if not args.sizes or any(n < 1 or n > 1025 for n in args.sizes):
        parser.error("sizes must be between 1 and 1025")
    package = (
        args.repo / "some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool"
    )
    candidate_file = package / "_provenance_reader.py"
    fixture_file = (
        args.repo / "some-implementations/riverhog/recovery/tests/test_prepared_provenance.py"
    )
    fixture = load_file("recovery_probe_fixture", fixture_file)
    modules = {}
    if args.mode in ("baseline", "both"):
        modules["baseline"] = load_reader("recovery_probe_baseline", args.baseline_file)
    if args.mode in ("prepared", "both"):
        modules["prepared"] = load_reader("recovery_probe_candidate", candidate_file)
    stages = {}
    if args.include_stage:
        for mode, reader_module in modules.items():
            name = reader_module.__package__
            # Resolve unchanged sibling recovery modules from the actual checkout,
            # while keeping the independently loaded old reader in this namespace.
            sys.modules[name].__path__ = [str(package)]
            source_file = (
                args.baseline_file.with_name("recovery.py")
                if mode == "baseline" else package / "recovery.py"
            )
            stages[mode] = load_file(name + ".recovery", source_file)
    cases = []
    for axis in args.axes:
        for n in args.sizes:
            dimensions = {"journals": 2, "segments": 2, "bindings": 1}
            if axis == "selected_structures":
                dimensions["bindings"] = max(args.sizes)
            dimensions[axis] = n
            cases.append((axis, dimensions))
    result = {
        "format": "riverhog-independent-reader-count-probe/v2",
        "scope": (
            "actual structural reader APIs, complete source consumption, "
            "plaintext fixture boundary; not encrypted or semantic recovery timing"
        ),
        "candidate_source_file_sha256": sha(candidate_file),
        "candidate_source_files_sha256": {
            str(path.relative_to(args.repo)): sha(path)
            for path in [candidate_file, package / "_provenance_index.py", package / "recovery.py"]
        },
        "baseline_source_file_sha256": sha(args.baseline_file),
        "baseline_stage_source_sha256": sha(args.baseline_file.with_name("recovery.py")),
        "fixture_sha256": sha(fixture_file),
        "probe_sha256": sha(Path(__file__)),
        "sqlite_instrumentation": (
            "progress handler every VM opcode and actual cursor fetches; "
            "EXPLAIN work excluded; no elapsed-time comparison"
        ),
        "observations": [],
        "stage_observations": [],
    }
    for axis, dimensions in cases:
        for mode, module in modules.items():
            observation = measure(mode, module, fixture, dimensions, args.repeats)
            observation["axis"] = axis
            result["observations"].append(observation)
        if args.include_stage and axis != "selected_structures":
            for mode, module in stages.items():
                observation = measure_stage(mode, module, fixture, dimensions)
                observation["axis"] = axis
                result["stage_observations"].append(observation)
    result["source_files_unchanged_during_probe"] = (
        result["fixture_sha256"] == sha(fixture_file)
        and all(
            digest == sha(args.repo / path)
            for path, digest in result["candidate_source_files_sha256"].items()
        )
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "output": str(args.output),
        "observations": len(result["observations"]),
        "stage_observations": len(result["stage_observations"]),
        "qualification": False,
    }))


if __name__ == "__main__":
    main()

