#!/usr/bin/env python3
"""Finite actual-caller work account for completed upload preparation.

Run identical saved journal bytes in separate baseline and candidate processes.
Counts include complete source capture, bounded preparation, every actual Native
turn and fenced terminal publication. Timings are intrusive diagnostics only.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import functools
import hashlib
import json
import os
import platform
import sqlite3
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def configure(repo):
    import tomllib

    project = tomllib.loads((repo / "pyproject.toml").read_text())
    paths = [repo]
    for pattern in project["tool"]["uv"]["workspace"]["members"]:
        paths.extend(path / "src" for path in repo.glob(pattern) if (path / "src").is_dir())
    sys.path[:0] = [str(path) for path in paths]
    os.chdir(repo)


def fixtures():
    from riverhog_provenance import (
        append_assertions,
        append_correction,
        assertion,
        assertion_reference,
        parse_journal,
        reference,
        validate_journal,
    )
    from tests.unit.test_journal_summary_consumers import source
    from tests.unit.test_staged_journal_range import JOURNAL

    base = source(journal_id=JOURNAL)
    raw = base
    records = []
    previous = None
    for frames in range(1, 5):
        if frames > 1:
            with validate_journal(raw) as summary:
                who = summary.tail.document["recorded_by_agent_id"]
                artifact = summary.graph["artifacts"][0]["id"]
            previous = assertion(
                "extension", who, subject=reference(artifact, "artifact"),
                property="urn:test:preparation-work", value={"type": "text", "value": str(frames)},
            )
            raw = append_assertions(raw, {"extensions": [previous]}, recorded_by_agent_id=who)
        if frames in (1, 2, 4):
            records.append((f"frames-{frames}", raw, 0))
    assert previous is not None
    with validate_journal(raw) as summary:
        retired = assertion_reference(summary, previous["assertion_id"])
    corrected = append_correction(raw, [retired], reason="finite correction fixture", recorded_by_agent_id=who)
    records.append(("correction-epochs-2", corrected, 0))
    records.append(("source-octets-65537", source(65537, journal_id=JOURNAL), 0))
    records.append(("unrelated-journals-1025", base, 1025))
    output = []
    for name, raw, unrelated in records:
        with validate_journal(raw) as summary:
            expected = dict(summary.anchor)
            frame_count = len(summary.frames)
        output.append({
            "name": name, "raw_base64": base64.b64encode(raw).decode(),
            "source_sha256": sha(raw), "source_bytes": len(raw), "frames": frame_count,
            "corrections": sum(frame.document["entry_kind"] == "correction" for frame in parse_journal(raw)),
            "unrelated": unrelated, "anchor": expected,
        })
    return output


class Recorder:
    def __init__(self):
        self.calls = Counter()
        self.sql = Counter()
        self.stack = []
        self.active = False
        self.native_turns = 0
        self.native_elapsed = 0.0
        self.owned_constructions = []
        self.owned_sessions = []

    def wrap(self, patch, target, name, label):
        original = getattr(target, name)

        @functools.wraps(original)
        def called(*args, **kwargs):
            observing = self.active
            if observing:
                self.calls[label] += 1
                self.stack.append(label)
                started = time.perf_counter()
            try:
                return original(*args, **kwargs)
            finally:
                if observing:
                    if label == "native.advance":
                        self.native_turns += 1
                        self.native_elapsed += time.perf_counter() - started
                    self.stack.pop()

        patch.setattr(target, name, called)

    def statement(self, connection, cursor, sql, parameters, context, many):
        if self.active:
            action = sql.strip().split(None, 1)[0].upper()
            self.sql["all." + action] += 1
            category = "native" if "native.advance" in self.stack else "service"
            self.sql[category + "." + action] += 1
            if self.stack:
                self.sql[self.stack[-1] + "." + action] += 1

    def transaction(self, event):
        def record(*args):
            if self.active:
                category = "native" if "native.advance" in self.stack else "service"
                self.sql["all." + event] += 1
                self.sql[category + "." + event] += 1
        return record


def run_case(case, root):
    import pytest
    from sqlalchemy import event
    from riverhog_core.services import collection_uploads as owner
    from riverhog_core.services import upload_journal_certificates as certificates
    from riverhog_core.services import upload_journal_epochs as epochs
    from riverhog_core.services import upload_journal_rank as rank
    from riverhog_core.services import upload_journal_replay as replay
    from riverhog_core.services import upload_journal_topology as topology
    from riverhog_core.services import upload_journal_union as union
    from riverhog_core.services import upload_journal_validation_jobs as jobs
    from riverhog_core.services import upload_journal_validation_owner as native
    from riverhog_core.services import upload_journal_source as source_owner
    from riverhog_provenance import journal_session, _session_checkpoint
    from tests.unit.test_journal_capture import retained
    from tests.unit.test_journal_validation_handoff import service_for, terminal
    from tests.unit.test_staged_journal_range import JOURNAL

    raw = base64.b64decode(case["raw_base64"])
    assert sha(raw) == case["source_sha256"]
    recorder = Recorder()
    turns = []
    methods = (
        (epochs, "advance_upload_journal_epoch_scan", "preparation.epoch"),
        (topology, "advance_upload_journal_topology_capture", "preparation.topology"),
        (union, "advance_upload_journal_epoch_union", "preparation.union"),
        (rank, "advance_upload_journal_epoch_rank", "preparation.rank"),
        (replay, "prepare_upload_journal_replay", "preparation.replay"),
        (replay, "_commit_binding", "prepare.binding_publication"),
        (epochs, "_begin_scan", "restore.epoch"),
        (topology, "_begin", "restore.topology"),
        (union, "_begin", "restore.union"),
        (rank, "_begin", "restore.rank"),
        (replay, "_probe", "current.replay_probe"),
        (certificates.UploadJournalGraphCertificates, "verify_current", "current.graph_certificate"),
        (source_owner.UploadJournalValidationSource, "verify_current", "current.source"),
        (jobs, "worker_current", "current.worker"),
        (jobs, "publish_progress", "publish.native_progress"),
        (native.UploadJournalValidationOwner, "advance", "native.advance"),
        (journal_session.JournalValidationSession, "advance", "native.session_advance"),
        (journal_session.JournalValidationSession, "progress", "native.progress"),
        (_session_checkpoint.ValidationLedger, "load", "native.checkpoint_load"),
        (_session_checkpoint.ValidationLedger, "_fence", "native.checkpoint_fence"),
        (owner, "_commit_upload_journal_validation", "publish.terminal"),
    )
    if hasattr(replay, "is_upload_journal_replay_prepared"):
        methods += ((replay, "is_upload_journal_replay_prepared", "admission.completed_preparation"),)
    with retained(root, raw) as (engine, factory):
        factory.configure(autoflush=False, expire_on_commit=False)
        if case["unrelated"]:
            from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
            from riverhog_core.checkpoint_sha256 import CheckpointSHA256
            from tests.unit.test_finalization_readiness_growth import clone_upload
            clone_upload(factory, 2)
            with factory() as session, session.begin():
                session.add_all(Journal(collection_id=2, journal_id=f"unrelated-{n}", bytes=0,
                    sha256="0" * 64, content_hash_state=CheckpointSHA256().export_state(), state="sealed")
                    for n in range(case["unrelated"]))
        service = service_for(factory)
        constructor = service._journal_validation_owner._construction_factory

        def construction():
            actual = constructor()
            recorder.owned_constructions.append(actual)
            return actual

        service._journal_validation_owner._construction_factory = construction
        receive_session = native._Slot.retain_session

        def retained_session(slot, session):
            recorder.owned_sessions.append(session)
            return receive_session(slot, session)

        callbacks = [("before_cursor_execute", recorder.statement), ("commit", recorder.transaction("COMMIT")), ("rollback", recorder.transaction("ROLLBACK"))]
        for kind, callback in callbacks:
            event.listen(engine, kind, callback)
        start = time.perf_counter()
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(native._Slot, "retain_session", retained_session)
                for target, name, label in methods:
                    recorder.wrap(patch, target, name, label)
                for turn in range(10000):
                    before_calls, before_sql = recorder.calls.copy(), recorder.sql.copy()
                    recorder.active = True
                    try:
                        service._advance_provenance_journal_validation_unit(1, JOURNAL)
                    finally:
                        recorder.active = False
                    assert engine.pool.checkedout() == 0
                    status = terminal(factory)
                    delta_calls = recorder.calls - before_calls
                    turns.append({"turn": turn, "calls": dict(delta_calls), "sql": dict(recorder.sql - before_sql), "state": status["state"]})
                    if status["state"] != "validating":
                        break
                else:
                    raise AssertionError("finite actual-caller fixture did not terminate")
                assert status["state"] == "sealed", status
                assert status["anchor"] == case["anchor"]
                assert recorder.native_turns > 1
        finally:
            for kind, callback in callbacks:
                event.remove(engine, kind, callback)
            service.close()
        assert service._journal_validation_owner._closed
        assert not service._journal_validation_owner._slots
        assert recorder.owned_constructions and all(frame.received for frame in recorder.owned_constructions)
        assert recorder.owned_sessions and all(session.storage_received for session in recorder.owned_sessions)
        assert engine.pool.checkedout() == 0
        native_directories = sorted(str(path.relative_to(root)) for path in root.rglob("snapshot.sqlite3"))
        result = {key: value for key, value in case.items() if key != "raw_base64"}
        result.update({
            "calls": dict(recorder.calls), "sql": dict(recorder.sql), "turns": turns,
            "service_turns": len(turns), "native_turns": recorder.native_turns,
            "complete_and_received_seconds_advisory": time.perf_counter() - start,
            "native_inclusive_seconds_advisory": recorder.native_elapsed,
            "original_constructions": len(recorder.owned_constructions),
            "original_constructions_received": all(frame.received for frame in recorder.owned_constructions),
            "original_sessions": len(recorder.owned_sessions),
            "original_sessions_storage_received": all(session.storage_received for session in recorder.owned_sessions),
            "catalog_connections_checked_out": engine.pool.checkedout(),
            "retained_native_snapshots_before_fixture_cleanup": native_directories,
            "exact_terminal_anchor": True,
        })
        engine.dispose()
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--create-fixtures", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--case", action="append")
    args = parser.parse_args()
    repo = args.repo.resolve()
    configure(repo)
    with tempfile.TemporaryDirectory(prefix="completed-preparation-probe-") as directory:
        root = Path(directory)
        os.environ["RIVERHOG_PROVENANCE_STATE_HOME"] = str(root / "user-state")
        from tests.support.runtime_config import write_test_runtime_config
        os.environ["RIVERHOG_CONFIG"] = str(write_test_runtime_config(root))
        if args.create_fixtures:
            if args.fixtures.exists():
                raise SystemExit("fixtures already exist")
            args.fixtures.write_text(json.dumps(fixtures(), indent=2) + "\n")
            return
        assert args.output is not None
        if args.output.exists():
            raise SystemExit("output already exists; retain prior observations")
        paths = ["riverhog/src/riverhog_core/services/" + name + ".py" for name in (
            "collection_uploads", "upload_journal_replay", "upload_journal_certificates", "upload_journal_validation_owner",
            "upload_journal_epochs", "upload_journal_topology", "upload_journal_union", "upload_journal_rank",
        )]
        import retained_resource
        import retained_resource._native as native_resources
        from riverhog_core.services import collection_uploads, upload_journal_replay
        from riverhog_provenance import journal_session
        loaded = {module.__name__: str(Path(module.__file__).resolve()) for module in (
            retained_resource, native_resources, collection_uploads, upload_journal_replay, journal_session,
        )}
        assert all(Path(path).is_relative_to(repo) for path in loaded.values())
        native_inputs = [repo / "packages/retained-resource/pyproject.toml", repo / "packages/retained-resource/hatch_build.py"]
        native_inputs.extend(sorted((repo / "packages/retained-resource/native").rglob("*")))
        result = {"probe_sha256": sha(Path(__file__).read_bytes()), "fixture_sha256": sha(args.fixtures.read_bytes()),
            "source_files": {path: sha((repo / path).read_bytes()) for path in paths},
            "environment": {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version, "platform": platform.system(),
                "loaded_owner_paths": loaded, "native_extension_sha256": sha(Path(native_resources.__file__).read_bytes()),
                "native_build_source_sha256": {str(path.relative_to(repo)): sha(path.read_bytes()) for path in native_inputs if path.is_file()},
                "native_budget": {"rows": journal_session.JournalValidationBudget().rows,
                    "octets": journal_session.JournalValidationBudget().octets,
                    "source_octets": journal_session.JournalValidationBudget().source_octets}},
            "observations": [], "scope": "actual validation unit and every Native turn; no throughput or production latency acceptance"}
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        for case in json.loads(args.fixtures.read_text()):
            if args.case and case["name"] not in args.case:
                continue
            result["observations"].append(run_case(case, root / case["name"]))
            args.output.write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps({"case": case["name"], "native_turns": result["observations"][-1]["native_turns"], "calls": result["observations"][-1]["calls"]}), flush=True)
        assert result["source_files"] == {path: sha((repo / path).read_bytes()) for path in paths}, "source changed during probe"
    assert not root.exists()
    result["all_probe_workspaces_removed"] = True
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
