"""Finite source-change counterexample for exact history-structure export."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import types
from dataclasses import replace
from pathlib import Path

from riverhog_archive_contracts import (
    RETAINED_HISTORY_EXTENT,
    MemberHistoryStore,
    RecordPage,
    history_record_page_object_path,
    provenance_structure_identity,
)
from riverhog_provenance import MemberHistoryClosure

BASELINE = "60612eded1e0e4e5188c28a84697435eb969fad0"
RELATIVE = "packages/riverhog-provenance/src/riverhog_provenance/history.py"
STORE_RELATIVE = (
    "packages/riverhog-archive-contracts/src/riverhog_archive_contracts/history_store.py"
)


def load_module(name, content):
    module = types.ModuleType(name)
    module.__package__ = name.rsplit(".", 1)[0]
    sys.modules[name] = module
    exec(compile(content, name, "exec"), module.__dict__)
    return module


def baseline_owner(repo):
    content = subprocess.check_output(["git", "show", BASELINE + ":" + RELATIVE], cwd=repo)
    store_content = subprocess.check_output(
        ["git", "show", BASELINE + ":" + STORE_RELATIVE], cwd=repo
    )
    module = load_module("riverhog_provenance._history_export_baseline", content)
    store = load_module("riverhog_archive_contracts._history_export_baseline_store", store_content)
    module.read_bounded_history_object = store.read_bounded_history_object
    return (
        module.MemberHistoryClosure,
        store.MemberHistoryStore,
        {
            "history_sha256": hashlib.sha256(content).hexdigest(),
            "store_sha256": hashlib.sha256(store_content).hexdigest(),
        },
    )


def probe(owner, store, *, second_member):
    from tests.support.neutral_history import history_fixture

    fixture = history_fixture(members=2, shared_ancestors=1, source_roots=2)
    ancestor = fixture.imports[0].source_history_sha256
    source = fixture.histories[ancestor]
    path = history_record_page_object_path(source.roots.records_sha256, 0)
    with owner(
        store(lambda path: (fixture.objects[path],)),
        lambda journal_id, end: (fixture.journals[journal_id][:end],),
        member_role="urn:test:member",
    ) as closure:
        closure.resolve(fixture.bindings[0], extent=RETAINED_HISTORY_EXTENT)
        original = fixture.objects[path]
        page = RecordPage.from_json_bytes(original)
        rows = tuple(
            row
            for row in page.records
            if row["value"]["journal"] == source.primary.journal.to_mapping()
        )
        assert len(rows) == 1 < len(page.records)
        changed = replace(page, records=rows).to_json_bytes()
        assert provenance_structure_identity(original).relative_path == path
        assert provenance_structure_identity(changed).relative_path == path
        fixture.objects[path] = changed
        result = {
            "removed_roots": len(page.records) - len(rows),
            "same_deterministic_path": True,
            "original_sha256": hashlib.sha256(original).hexdigest(),
            "changed_sha256": hashlib.sha256(changed).hexdigest(),
            "second_member_requested": second_member,
        }
        if second_member:
            try:
                closure.resolve(fixture.bindings[1], extent=RETAINED_HISTORY_EXTENT)
            except ValueError as exc:
                result["second_member_error"] = str(exc)
            else:
                result["second_member_error"] = None
        try:
            exported = tuple(closure.structure_objects())
        except ValueError as exc:
            result["export_error"] = str(exc)
            result["changed_bytes_exported"] = False
        else:
            result["export_error"] = None
            result["changed_bytes_exported"] = changed in exported
        try:
            closure.store.descriptor(fixture.proofs[ancestor].binding)
        except ValueError as exc:
            result["full_descriptor_error"] = str(exc)
        else:
            raise AssertionError("mutated complete set was admitted")
        root = closure._root
    assert not root.exists()
    result["closure_scratch_removed"] = True
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--before-fix", type=Path)
    args = parser.parse_args()
    repo = args.repo.resolve()
    sys.path.insert(0, str(repo))
    old, old_store, old_hashes = baseline_owner(repo)
    owners = [("baseline", old, old_store), ("candidate", MemberHistoryClosure, MemberHistoryStore)]
    if args.before_fix:
        before = args.before_fix.read_bytes()
        module = load_module("riverhog_provenance._history_export_before_fix", before)
        owners.insert(1, ("before-export-fix", module.MemberHistoryClosure, MemberHistoryStore))
    data = {
        "baseline_commit": BASELINE,
        "baseline_source": old_hashes,
        "candidate_source": {
            "history_sha256": hashlib.sha256((repo / RELATIVE).read_bytes()).hexdigest(),
            "store_sha256": hashlib.sha256((repo / STORE_RELATIVE).read_bytes()).hexdigest(),
        },
        "scope": (
            "real neutral Store/Closure and complete fixture; "
            "no encrypted provider or product timing"
        ),
        "observations": [
            {"owner": name, **probe(owner, store, second_member=second)}
            for name, owner, store in owners
            for second in (False, True)
        ],
    }
    if args.before_fix:
        data["before_export_fix_history_sha256"] = hashlib.sha256(before).hexdigest()
    args.output.write_text(json.dumps(data, indent=2) + "\n")
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
