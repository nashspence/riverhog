"""Reference-only finite observation of existing synchronous owner calls."""

from __future__ import annotations

import atexit
import hashlib
import json
import os
import sys
import threading
import time
from contextlib import contextmanager
from functools import wraps
from pathlib import Path

MAX_THREADS, MAX_BUCKETS, MAX_DEPTH, MAX_PHASES = 4096, 1024, 128, 128
PHASES = {
    "publication",
    "append-inputs",
    "primary-history-received",
    "member-histories-staged",
    "finalization",
    "publication-received",
    "verification",
    "verification-received",
    "cleanup-received",
}
CACHES = {"fresh-native-reader", "warm-native-reader", "evicted-native-reader"}


class Recorder:
    def __init__(self):
        self.start_wall, self.start_cpu = time.perf_counter_ns(), time.process_time_ns()
        self.local, self.lock = threading.local(), threading.Lock()
        self.threads, self.phases, self.hooks = [], [], []
        self.phase, self.invalid = "bootstrap", set()
        self.observer_wall = self.observer_cpu = 0
        self.phase_start = (self.start_wall, self.start_cpu)

    def state(self):
        if hasattr(self.local, "state"):
            return self.local.state
        with self.lock:
            if len(self.threads) >= MAX_THREADS:
                self.invalid.add("thread-budget-exceeded")
                self.local.state = None
                return None
            actual = threading.current_thread()
            name = actual.name
            role = next(
                (
                    p
                    for p in (
                        "MainThread",
                        "AnyIO worker thread",
                        "physical-execution",
                        "asyncio-portal",
                        "ThreadPoolExecutor",
                    )
                    if name.startswith(p)
                ),
                "other",
            )
            state = {
                "lifetime": len(self.threads),
                "ident": threading.get_ident(),
                "native_id": threading.get_native_id(),
                "role": role,
                "stack": [],
                "buckets": {},
                "observer_wall_ns": 0,
                "observer_cpu_ns": 0,
            }
            self.threads.append(state)
            self.local.state = state
            return state

    @contextmanager
    def span(self, label):
        entry_wall, entry_cpu = time.perf_counter_ns(), time.thread_time_ns()
        state = self.state()
        if state is None or len(state["stack"]) >= MAX_DEPTH:
            self.invalid.add("span-budget-exceeded")
            yield None
            return
        frame = {
            "label": label,
            "phase": self.phase,
            "children_wall": 0,
            "children_cpu": 0,
            "rows": 0,
            "outcome": "returned",
        }
        state["stack"].append(frame)
        start_wall, start_cpu = time.perf_counter_ns(), time.thread_time_ns()
        state["observer_wall_ns"] += start_wall - entry_wall
        state["observer_cpu_ns"] += start_cpu - entry_cpu
        try:
            yield frame
        except StopIteration:
            frame["outcome"] = "exhausted"
            raise
        except BaseException:
            frame["outcome"] = "raised"
            raise
        finally:
            end_wall, end_cpu = time.perf_counter_ns(), time.thread_time_ns()
            if not state["stack"] or state["stack"][-1] is not frame:
                self.invalid.add("unbalanced-spans")
            else:
                state["stack"].pop()
            wall, cpu = end_wall - start_wall, end_cpu - start_cpu
            key = (frame["phase"], label)
            if key not in state["buckets"] and len(state["buckets"]) >= MAX_BUCKETS:
                self.invalid.add("bucket-budget-exceeded")
            else:
                row = state["buckets"].setdefault(
                    key,
                    dict.fromkeys(
                        (
                            "calls",
                            "raised",
                            "exhausted",
                            "rows",
                            "wall_ns",
                            "cpu_ns",
                            "exclusive_wall_ns",
                            "exclusive_cpu_ns",
                            "cross_phase_calls",
                        ),
                        0,
                    ),
                )
                row["calls"] += 1
                row["raised"] += frame["outcome"] == "raised"
                row["exhausted"] += frame["outcome"] == "exhausted"
                row["rows"] += frame["rows"]
                row["wall_ns"] += wall
                row["cpu_ns"] += cpu
                row["exclusive_wall_ns"] += wall - frame["children_wall"]
                row["exclusive_cpu_ns"] += cpu - frame["children_cpu"]
                row["cross_phase_calls"] += frame["phase"] != self.phase
            if state["stack"]:
                state["stack"][-1]["children_wall"] += wall
                state["stack"][-1]["children_cpu"] += cpu
            state["observer_wall_ns"] += time.perf_counter_ns() - end_wall
            state["observer_cpu_ns"] += time.thread_time_ns() - end_cpu

    def phase_event(self, event):
        label = event.get("phase")
        if label not in PHASES:
            self.invalid.add("unknown-phase")
            return
        if label == "append-inputs":
            label = (
                "primary-history"
                if event.get("completed") == event.get("members")
                else "append-inputs"
            )
        elif label == "primary-history-received":
            label = "history-preparation-and-staging"
        elif label == "verification":
            cache = event.get("native_cache_state")
            if cache not in CACHES:
                self.invalid.add("unknown-reader-state")
                return
            label = "verification-" + cache
        if label == self.phase:
            return
        self.close_phase(label)

    def close_phase(self, next_label):
        now = time.perf_counter_ns(), time.process_time_ns()
        if len(self.phases) >= MAX_PHASES:
            self.invalid.add("phase-budget-exceeded")
        else:
            self.phases.append(
                {
                    "phase": self.phase,
                    "wall_ns": now[0] - self.phase_start[0],
                    "process_cpu_ns": now[1] - self.phase_start[1],
                }
            )
        self.phase, self.phase_start = next_label, now

    def wrap(self, original, label, rows=False, phase_argument=None):
        @wraps(original)
        def observed(*args, **kwargs):
            if phase_argument and kwargs.get(phase_argument) is not None:
                callback = kwargs[phase_argument]

                def progress(event):
                    self.phase_event(event)
                    return callback(event)

                kwargs[phase_argument] = progress
            with self.span(label) as frame:
                result = original(*args, **kwargs)
                if rows and frame is not None:
                    frame["rows"] = (
                        len(result)
                        if label.endswith(("fetchall", "fetchmany"))
                        else int(label.endswith("__next__") or result is not None)
                    )
                return result

        return observed

    def patch(self, owner, name, label, **options):
        original = getattr(owner, name)
        setattr(owner, name, self.wrap(original, label, **options))
        self.hooks.append(label)

    def report(self):
        self.close_phase("received-exit")
        threads, covered = [], 0
        for state in self.threads:
            if state["stack"]:
                self.invalid.add("unfinished-spans")
            buckets = [
                {"phase": key[0], "operation": key[1], **value}
                for key, value in sorted(state["buckets"].items())
            ]
            subtotal = sum(row["exclusive_cpu_ns"] for row in buckets)
            covered += subtotal
            threads.append(
                {k: v for k, v in state.items() if k not in ("stack", "buckets")}
                | {"buckets": buckets, "covered_exclusive_cpu_ns": subtotal}
            )
        cpu = sum(phase["process_cpu_ns"] for phase in self.phases)
        return {
            "format": "riverhog-991-attribution/v1",
            "attribution_valid": not self.invalid,
            "invalid_reasons": sorted(self.invalid),
            "pid": os.getpid(),
            "hooks": self.hooks,
            "phases": self.phases,
            "threads": threads,
            "elapsed_ns": sum(p["wall_ns"] for p in self.phases),
            "process_cpu_ns": cpu,
            "covered_exclusive_cpu_ns": covered,
            "unattributed_process_cpu_ns": cpu - covered,
            "budgets": {
                "threads": MAX_THREADS,
                "buckets_per_thread": MAX_BUCKETS,
                "nesting": MAX_DEPTH,
                "phases": MAX_PHASES,
            },
            "observation_window": (
                "recorder construction after dependency imports through atexit receipt; "
                "excludes earlier helper startup"
            ),
            "limitations": [
                "Instrumented diagnostic; not a matching throughput baseline.",
                "Thread CPU covers instrumented spans only, not complete thread lifetime.",
                "Exclusive wall is same-thread nesting accounting, not concurrent critical path.",
                "Cross-phase spans are attributed to start phase; phase CPU is process-wide.",
                "Catalog cursor events omit fetch, commit and pool/connection work.",
                "Native cursor counters cover sqlite_admission objects only; "
                "raw disposable JournalStore/closure/worklist SQLite is uncovered.",
                "Session and ledger spans may include uncovered raw SQLite; "
                "their CPU cannot be assigned to measured admitted SQL.",
                "Native rows are returned rows, not engine row visits or VM steps.",
                "One executemany call may execute many statements; parameters are not consumed.",
                "Observer bookkeeping overlaps parent spans and is not subtracted.",
                "Elapsed minus CPU does not identify a lock, IO or scheduler wait.",
            ],
        }


def install_sqlite(recorder):
    import sqlite_admission as owner

    for method in (
        "execute",
        "executemany",
        "executescript",
        "fetchone",
        "fetchmany",
        "fetchall",
        "__next__",
    ):
        recorder.patch(
            owner._Cursor,
            method,
            "native.admitted.cursor." + method,
            rows=method.startswith("fetch") or method == "__next__",
        )
    for method in ("commit", "rollback"):
        recorder.patch(owner._Connection, method, "native.admitted.connection." + method)


def install_catalog(recorder):
    from sqlalchemy import event
    from sqlalchemy.engine import Engine

    def before(_conn, _cursor, _statement, _parameters, context, _many):
        scope = recorder.span("catalog.cursor.execute")
        scope.__enter__()
        context._riverhog_991_scope = scope

    def after(_conn, _cursor, _statement, _parameters, context, _many):
        scope, context._riverhog_991_scope = context._riverhog_991_scope, None
        scope.__exit__(None, None, None)

    def failed(context):
        execution = context.execution_context
        scope = getattr(execution, "_riverhog_991_scope", None)
        if scope is not None:
            execution._riverhog_991_scope = None
            error = context.original_exception
            scope.__exit__(type(error), error, error.__traceback__)

    for name, callback in (
        ("before_cursor_execute", before),
        ("after_cursor_execute", after),
        ("handle_error", failed),
    ):
        event.listen(Engine, name, callback)
        recorder.hooks.append("sqlalchemy.Engine." + name)
    return (
        ("before_cursor_execute", before),
        ("after_cursor_execute", after),
        ("handle_error", failed),
    )


def calibration():
    recorder = Recorder()

    def noop():
        return None

    observed = recorder.wrap(noop, "calibration")
    result = []
    for _ in range(3):
        row = {}
        for label, operation in (("plain", noop), ("observed", observed)):
            wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
            for _ in range(2000):
                operation()
            row[label] = {
                "wall_ns": time.perf_counter_ns() - wall,
                "cpu_ns": time.thread_time_ns() - cpu,
                "calls": 2000,
            }
        result.append(row)
    return {"rounds": result, "scope": "same-thread empty call; no production correction"}


def install(destination):
    bootstrap = time.perf_counter_ns(), time.process_time_ns()
    micro = calibration()
    from fastapi.testclient import TestClient
    from riverhog_client.producer import IncrementalCollectionProducer
    from riverhog_provenance._session_checkpoint import ValidationLedger
    from riverhog_provenance.journal_session import JournalValidationSession

    from scripts.validation_identity import source_identity
    from tests.support.qualification import canonical_history_scale as history
    from tests.support.qualification import publication_evidence as evidence

    recorder = Recorder()
    source = source_identity(Path.cwd())
    install_sqlite(recorder)
    install_catalog(recorder)
    # runpy executes publication_profile as __main__. Patch its imported owners,
    # not a second imported copy of the module that would never execute.
    publication = recorder.wrap(
        history.publish_shared_history, "publication.owner", phase_argument="progress"
    )
    verification = recorder.wrap(evidence.measure_published_history, "verification.owner")
    verification_calls = 0

    def publish(*args, **kwargs):
        recorder.close_phase("publication")
        try:
            return publication(*args, **kwargs)
        finally:
            recorder.close_phase("post-publication")

    def verify(*args, **kwargs):
        nonlocal verification_calls
        verification_calls += 1
        if verification_calls > 3:
            recorder.invalid.add("unexpected-verification-count")
        recorder.close_phase("verification-pass-" + str(min(verification_calls, 4)))
        try:
            return verification(*args, **kwargs)
        finally:
            recorder.close_phase("post-verification-and-cleanup")

    history.publish_shared_history = publish
    evidence.measure_published_history = verify
    recorder.hooks.extend(("publication.owner", "verification.owner"))
    recorder.patch(evidence, "verify_stored_publication", "verification.ciphertext")
    recorder.patch(IncrementalCollectionProducer, "append_inputs", "producer.append_inputs")
    recorder.patch(IncrementalCollectionProducer, "finish", "producer.finish")
    recorder.patch(TestClient, "request", "transport.TestClient.request")
    recorder.patch(JournalValidationSession, "advance", "validation.advance")
    for name in ("load", "_fence", "_publish", "_prefix"):
        recorder.patch(ValidationLedger, name, "validation.ledger." + name)
    observer = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(Path(__file__).parent.glob("*.py"))
    }
    startup = {
        "wall_ns": time.perf_counter_ns() - bootstrap[0],
        "process_cpu_ns": time.process_time_ns() - bootstrap[1],
    }
    destination.mkdir(parents=True, exist_ok=True)

    def receive():
        if verification_calls != 3:
            recorder.invalid.add("incomplete-verification-count")
        report = recorder.report() | {
            "source_start": source,
            "source_end": source_identity(Path.cwd()),
            "observer_sha256": observer,
            "bootstrap_including_calibration": startup,
            "micro_calibration": micro,
            "verification_calls": verification_calls,
            "reader_label_rule": (
                "Pass ordinals must be reconciled to the maintained profile's "
                "fixed fresh/warm/evicted order."
            ),
            "python": sys.version,
            "clocks": {
                name: vars(time.get_clock_info(name))
                for name in ("perf_counter", "process_time", "thread_time")
            },
        }
        rows = [row for thread in report["threads"] for row in thread["buckets"]]
        report["scope_calls"] = {
            prefix: sum(row["calls"] for row in rows if row["operation"].startswith(prefix))
            for prefix in (
                "catalog.cursor",
                "native.admitted",
                "validation.advance",
                "validation.ledger",
                "producer.",
                "transport.",
                "verification.",
            )
        }
        report["zero_observed_scopes"] = [
            name for name, count in report["scope_calls"].items() if not count
        ]
        report["zero_scope_rule"] = (
            "A zero is a coverage/activation question, not proof the work is absent; "
            "raw SQLite is intentionally uncovered."
        )
        if report["source_start"] != report["source_end"]:
            report["attribution_valid"] = False
            report["invalid_reasons"].append("source-changed")
        target = destination / ("diagnostic-" + str(os.getpid()) + ".json")
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
        temporary.replace(target)

    atexit.register(receive)
    return recorder
