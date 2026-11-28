#!/usr/bin/env python3
"""Compare exact neutral-history owners on finite maintained fixtures.

The primitive arm uses authenticated plaintext boundaries and real neutral
validators. The optional encrypted arm runs the actual independent recovery
caller with official age/batchpass and the maintained low-KDF fixture. Neither
arm is a production wall-clock qualification.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.util
import json
import os
import platform
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

BASE = "60612eded1e0e4e5188c28a84697435eb969fad0"
STORE_PATH = "packages/riverhog-archive-contracts/src/riverhog_archive_contracts/history_store.py"
CLOSURE_PATH = "packages/riverhog-provenance/src/riverhog_provenance/history.py"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def load_source(repo, path, name, directory):
    raw = subprocess.check_output(["git", "show", f"{BASE}:{path}"], cwd=repo)
    target = directory / (name.rsplit(".", 1)[-1] + ".py")
    target.write_bytes(raw)
    spec = importlib.util.spec_from_file_location(name, target)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module, sha(raw)


class Reads:
    def __init__(self, values):
        self.values, self.opens, self.octets = values, Counter(), Counter()
        self.closed, self.active, self.peak = 0, 0, 0

    def read(self, key, end=None):
        self.opens[key] += 1
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            raw = self.values[key][:end]
            self.octets[key] += len(raw)
            yield raw
        finally:
            self.closed += 1
            self.active -= 1

    def result(self):
        assert self.active == 0 and self.closed == sum(self.opens.values())
        return {
            "opens": sum(self.opens.values()),
            "distinct": len(self.opens),
            "octets": sum(self.octets.values()),
            "closed": self.closed,
            "active": self.active,
            "peak": self.peak,
            "per_identity": dict(sorted(self.opens.items())),
        }


def primitive(case, store_cls, closure_cls):
    from riverhog_archive_contracts import RETAINED_HISTORY_EXTENT, provenance_structure_identity
    from tests.support.sqlite_admission import Population

    objects, journals = Reads(case.objects), Reads(case.journals)
    population = Population(limit=40)
    retained = []
    statements = defaultdict(Counter)

    def connection_factory(database, **options):
        db = population(database, **options)
        role = Path(database).name
        db.set_trace_callback(lambda statement: statements[role].update([statement.split()[0].upper()]))
        return db

    store = store_cls(
        objects.read, connection_factory=connection_factory, on_storage_open=retained.append
    )
    closure = closure_cls(
        store,
        journals.read,
        member_role="urn:test:member",
        connection_factory=connection_factory,
        on_storage_open=retained.append,
        journal_connection_factory=connection_factory,
        on_journal_storage_open=retained.append,
    )
    initialized = {role: counts.copy() for role, counts in statements.items()}
    with closure:
        for binding in case.bindings:
            closure.resolve(binding, extent=RETAINED_HISTORY_EXTENT)
        resolved = dict(objects.opens)
        resolved_statements = {
            role: dict(counts - initialized.get(role, Counter()))
            for role, counts in statements.items()
        }
        actual = {
            provenance_structure_identity(raw).relative_path: raw
            for raw in closure.structure_objects()
        }
        assert actual == case.objects
        actual_anchors = set(closure.journal_anchors())
        assert actual_anchors == set(case.expected_anchors)
        for journal in case.journals:
            assert closure.contains_journal(journal)
        assert not closure._cursors
    assert population.active == 0
    assert all(db.close_calls == 1 for db in population.connections)
    assert all(not path.parent.exists() for path, _options in population.attempts)
    # Derive categories from exact identities, without relying on path spelling.
    from riverhog_archive_contracts import member_history_object_path, source_binding_proof_object_path

    descriptor_paths = {member_history_object_path(identity) for identity in case.histories}
    proof_paths = {source_binding_proof_object_path(proof.identity) for proof in case.proofs.values()}
    categories = {
        "descriptors": descriptor_paths,
        "proofs": proof_paths,
        "pages": set(case.objects) - descriptor_paths - proof_paths,
    }
    return {
        "resolve_reads": {
            category: sum(resolved.get(path, 0) for path in paths)
            for category, paths in categories.items()
        },
        "resolve_and_export_reads": {
            category: sum(objects.opens.get(path, 0) for path in paths)
            for category, paths in categories.items()
        },
        "source_objects": objects.result(),
        "source_journals": journals.result(),
        "sqlite": {
            "opens": len(population.connections),
            "peak": population.peak,
            "active": population.active,
            "all_original_connections_closed_once": True,
            "all_owned_directories_removed": True,
            "resolve_statements_by_role": resolved_statements,
        },
        "exact_structures": len(actual),
        "exact_anchors": len(actual_anchors),
        "complete_output_equal": True,
    }


@contextlib.contextmanager
def replaced(target, **attributes):
    old = {key: getattr(target, key) for key in attributes}
    for key, value in attributes.items():
        setattr(target, key, value)
    try:
        yield
    finally:
        for key, value in old.items():
            setattr(target, key, value)


def encrypted(case, archive, output, store_cls, closure_cls):
    import a_riverhog_recovery_tool._archive_io as io
    import a_riverhog_recovery_tool.recovery as recovery
    from tests.support.qualification.recovery_archive import PASSPHRASE, PASSPHRASE_ID

    decrypt, run = io.age_decrypt, io.subprocess.run
    calls, received = Counter(), []

    def observed(source, destination, **options):
        calls[str(source.relative_to(archive))] += 1
        return decrypt(source, destination, **options)

    def subprocess_run(*args, **options):
        result = run(*args, **options)
        received.append(result.returncode)
        return result

    with (
        replaced(recovery, MemberHistoryStore=store_cls, MemberHistoryClosure=closure_cls),
        replaced(io, age_decrypt=observed),
        replaced(io.subprocess, run=subprocess_run),
    ):
        summary = recovery.recover_archive(
            archive, output, passphrases={PASSPHRASE_ID: PASSPHRASE}
        )
    records = [json.loads(line) for line in (output / "recovery-members.jsonseq").read_bytes().splitlines()]
    assert len(records) == len(case.members) == summary.artifacts
    for row in records:
        assert output.joinpath(*row["components"]).read_bytes() == case.members[row["artifact_id"]]
    for path, raw in case.history_objects.items():
        assert (output / "structure" / path.removesuffix(".age")).read_bytes() == raw
    for raw in case.journals.values():
        assert (output / "provenance" / "journals" / (sha(raw) + ".jsonseq")).read_bytes() == raw
    assert json.loads((output / "recovery.json").read_bytes())["complete"] is True
    assert len(received) == sum(calls.values()) and set(received) == {0}
    assert not (output.parent / f".{output.name}.riverhog-recovery").exists()
    return {
        "decryptions": sum(calls.values()),
        "history_decryptions": sum(value for path, value in calls.items() if path in case.history_objects),
        "per_object": dict(sorted(calls.items())),
        "age_processes_received": len(received),
        "age_return_codes": dict(Counter(received)),
        "exact_payload_members": summary.artifacts,
        "exact_history_objects": len(case.history_objects),
        "exact_journals": len(case.journals),
        "complete_receipt": True,
        "owned_recovery_scratch_removed": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--mode", choices=("primitive", "encrypted", "all"), default="all")
    args = parser.parse_args()
    repo, output = args.repo.resolve(), args.output.resolve()
    if output.exists():
        raise SystemExit("output already exists; retain prior observations")
    sys.path.insert(0, str(repo))
    os.chdir(repo)
    from riverhog_archive_contracts.history_store import MemberHistoryStore
    from riverhog_provenance.history import MemberHistoryClosure
    from tests.support.neutral_history import history_fixture

    result = {
        "baseline": BASE,
        "candidate_files": {path: sha((repo / path).read_bytes()) for path in (STORE_PATH, CLOSURE_PATH)},
        "probe_sha256": sha(Path(__file__).read_bytes()),
        "fixture_sha256": sha((repo / "tests/support/neutral_history.py").read_bytes()),
        "environment": {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version, "platform": platform.system()},
        "observations": [],
    }

    def save():
        output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")

    with tempfile.TemporaryDirectory(prefix="neutral-history-reference-") as directory:
        work = Path(directory)
        baseline_store, store_hash = load_source(repo, STORE_PATH, "riverhog_archive_contracts._reference_baseline_store", work)
        baseline_closure, closure_hash = load_source(repo, CLOSURE_PATH, "riverhog_provenance._reference_baseline_closure", work)
        baseline_closure.MemberHistoryStore = baseline_store.MemberHistoryStore
        baseline_closure.read_bounded_history_object = baseline_store.read_bounded_history_object
        result["baseline_files"] = {STORE_PATH: store_hash, CLOSURE_PATH: closure_hash}
        owners = {
            "baseline": (baseline_store.MemberHistoryStore, baseline_closure.MemberHistoryClosure),
            "candidate": (MemberHistoryStore, MemberHistoryClosure),
        }
        cases = [
            ("members", {"members": value}) for value in (1, 2, 4)
        ] + [
            ("root-pages", {"roots": value}) for value in (2, 4, 129)
        ] + [
            ("shared-root-set", {"members": 4, "roots": 4})
        ] + [
            ("private-imports", {"private_imports": value}) for value in (1, 2, 4)
        ] + [
            ("shared-ancestors", {"members": 4, "shared_ancestors": value, "source_roots": 4})
            for value in (1, 2, 4)
        ] + [
            ("ancestor-root-pages", {"members": 4, "shared_ancestors": 1, "source_roots": value})
            for value in (1, 129)
        ] + [
            ("history-depth", {"shared_ancestors": 1, "depth": value}) for value in (2, 4)
        ]
        if args.mode in ("primitive", "all"):
            for axis, dimensions in cases:
                case = history_fixture(**dimensions)
                shared_sets = Counter(history.roots.records_sha256 for history in case.histories.values())
                for mode, (store_cls, closure_cls) in owners.items():
                    row = {"kind": "primitive", "axis": axis, "dimensions": dimensions, "mode": mode,
                           "history_count": len(case.histories), "import_edges": len(case.imports),
                           "root_set_sharing": sorted(shared_sets.values()), "status": "started"}
                    result["observations"].append(row)
                    save()
                    started = time.perf_counter()
                    try:
                        row["result"] = primitive(case, store_cls, closure_cls)
                        row["status"] = "passed"
                    except BaseException as exc:
                        row.update(status="failed", error=f"{type(exc).__name__}: {exc}")
                        raise
                    finally:
                        row["elapsed_seconds_advisory"] = time.perf_counter() - started
                        save()
                    print(axis, dimensions, mode, row["result"]["resolve_reads"], flush=True)
        if args.mode in ("encrypted", "all"):
            from tests.support.qualification.recovery_archive import write_archive

            result["official_tools"] = {
                command: subprocess.check_output([command, "--version"], text=True).strip()
                for command in ("age", "age-plugin-batchpass")
            }
            assert set(result["official_tools"].values()) == {"v1.3.1"}
            for members in (1, 2, 4):
                archive = work / f"archive-{members}"
                payloads = {f"{number + 1:064x}": b"fixture payload" for number in range(members)}
                case = write_archive(archive, members=payloads, hints={key: None for key in payloads})
                for mode, (store_cls, closure_cls) in owners.items():
                    row = {"kind": "encrypted-recovery", "members": members, "mode": mode, "fixture_kdf_log_n": 1, "status": "started"}
                    result["observations"].append(row)
                    save()
                    started = time.perf_counter()
                    try:
                        row["result"] = encrypted(case, archive, work / f"output-{members}-{mode}", store_cls, closure_cls)
                        row["status"] = "passed"
                    except BaseException as exc:
                        row.update(status="failed", error=f"{type(exc).__name__}: {exc}")
                        raise
                    finally:
                        row["elapsed_seconds_advisory"] = time.perf_counter() - started
                        save()
                    print("encrypted-recovery", members, mode, row["result"]["decryptions"], flush=True)
    assert not work.exists()
    assert result["candidate_files"] == {path: sha((repo / path).read_bytes()) for path in (STORE_PATH, CLOSURE_PATH)}
    result["probe_workspace_removed"] = True
    result["candidate_unchanged_during_probe"] = True
    save()


if __name__ == "__main__":
    main()
