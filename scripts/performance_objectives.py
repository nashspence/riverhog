#!/usr/bin/env python3
"""Non-contractual performance objectives, bookkeeping, and result comparisons.

No application or contract generator imports this module. Existing benchmark/test
callers decide whether a miss is diagnostic or fails their existing check.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
PAGE = Path("qualification/performance/README.md")
SCHEMA = "riverhog-performance-objectives/v1"
RESULT_SCHEMA = "riverhog-performance-sample/v1"
CONTEXT_SCHEMA = "riverhog-performance-context/v1"
NOTICE = (
    'Non-contractual engineering objectives and observations only. These records '
        'do not '
    'create, extend, interpret, override, or relax any external contract. A '
        'recorded target '
    "is not automatically a release gate. Related checks retain their independently "
    'defined role. No passing execution or external guarantee is implied by this '
        'page.'
)

# Retained existing tolerances, not theoretical hardware capabilities. A target
# change is an explicit source edit. Do not duplicate these values in consumers.
BUDGETS: dict[str, dict[str, float | int]] = {
    "goodput": {"minimum_reference_fraction": 0.90},
    "page-stream-peak": {"maximum": 32 * 1024 * 1024},
    "http-peak": {"maximum": 64 * 1024 * 1024},
    "plan-work": {"indexed_factor": 4, "linear_factor": 1.5, "slack_rows": 1000},
    "plan-latency": {"minimum_factor": 8, "growth_factor": 4, "slack_ms": 100},
    "memory-growth": {"factor": 4, "slack_bytes": 8 * 1024 * 1024},
    "stream-latency": {"minimum_factor": 4, "growth_factor": 1.5, "slack_ms_per_row": 0.5},
    "listener-observation": {"exclusive_maximum": 4 * 1024 * 1024},
    "listener-runnable": {"exclusive_maximum": 1024 * 1024},
    "plan-temp-io": {"maximum": 0},
}

# These are benchmark dimensions, not assertions that all combinations have run.
SCENARIO_OPERATIONS: dict[str, frozenset[str]] = {
    "riverhog-ingress": frozenset({"pack_upload_open", "pack_write_segment", "raw_upload_open",
            "raw_write_segment"}),
    "stove0-derived-publication": frozenset({"pack_upload_open", "pack_write_segment",
            "raw_upload_open", "raw_write_segment"}),
    "stove0-input-read": frozenset({"pack_retrieval_member", "pack_retrieval_range",
            "raw_retrieval_part"}),
    "riverhog-retrieval": frozenset({"pack_retrieval_member", "pack_retrieval_range",
            "raw_retrieval_part"}),
    "reference-recovery": frozenset(),
    "archive-upload": frozenset({"pack_upload_open", "pack_write_segment", "raw_upload_open",
            "raw_write_segment"}),
    "archive-retrieval": frozenset({"pack_retrieval_member", "pack_retrieval_range",
            "raw_retrieval_part", "retrieval_cache_hydration"}),
    "archive-replication": frozenset({"archive_copy_segment", "archive_copy_object"}),
}
WORKLOADS = ("large-file", "many-small-files", "resume")
NETWORK_SCENARIOS = frozenset(SCENARIO_OPERATIONS) - {"reference-recovery"}


@dataclass(frozen=True)
class Objective:
    id: str
    metric: str
    unit: str
    rule: str
    budget: str
    scope: str
    existing_use: str
    sources: tuple[str, ...]


_DB = "scripts/database_qualification.py"
_LISTENER = "reference/gogurt/application/tests/test_listener.py"
OBJECTIVES = (
    Objective(
        id='transfer-goodput',
        metric='newly completed useful payload / end-to-end wall time',
        unit='bytes/s',
        rule='reference-ratio',
        budget='goodput',
        scope=(
                'Seven network scenario labels x three workload labels in '
                'SCENARIO_OPERATIONS/WORKLOADS. Compare only matched, verified completion '
                'measurements in one declared comparison session. Counts exclude '
                'already-completed resume work and retransmitted copies. The reference is a '
                'measured comparable completion path, not line rate, an asserted capacity, or '
                'an automatic previous-release baseline.'
        ),
        existing_use='report-only',
        sources=(
            'scripts/transfer_profile.py::main',
        ),
    ),
    Objective(
        id='storage-upload-goodput',
        metric='completed stored-object payload / begin-through-complete wall time',
        unit='bytes/s',
        rule='reference-ratio',
        budget='goodput',
        scope=(
                'One bounded synthetic object through the public adapter. Includes admission, '
                'all segment writes, segment traversal and validated completion. Post-upload '
                'readback verifies the payload but is outside the upload timer. Compare to '
                'upload only; exclude setup and cleanup equally on both sides.'
        ),
        existing_use='report-only',
        sources=(
            'tests/harness/storage_adapter_goodput_probe.py::run',
        ),
    ),
    Objective(
        id='storage-read-goodput',
        metric='verified stored-object payload / read-and-hash wall time',
        unit='bytes/s',
        rule='reference-ratio',
        budget='goodput',
        scope=(
                'Read the just-written exact revision, including hashing and stream close. '
                'This is a read-after-write workload, not an uncached retrieval promise. '
                'Compare to read only; no shared upload/read baseline.'
        ),
        existing_use='report-only',
        sources=(
            'tests/harness/storage_adapter_goodput_probe.py::run',
        ),
    ),
    Objective(
        id='database-page-stream-peak',
        metric='peak traced Python allocations during SQL stream',
        unit='bytes',
        rule='maximum',
        budget='page-stream-peak',
        scope=(
                'Current PostgreSQL selector fixture, 100-row fetch chunks, measured at 4096 '
                'and 65536 relation rows. tracemalloc is not RSS or database-server memory.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_measure_page_stream',
        ),
    ),
    Objective(
        id='database-http-peak',
        metric='peak traced Python allocations during official-client traversal',
        unit='bytes',
        rule='maximum',
        budget='http-peak',
        scope=(
                'Current bounded-page, catalog-sync and inventory fixture paths in '
                '_measure_http_path. Process-wide traced Python allocations, not per-request '
                'RSS or a deployment memory allowance.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_measure_http_path',
        ),
    ),
    Objective(
        id='database-indexed-work-growth',
        metric='sum of plan Actual Rows x Actual Loops',
        unit='plan-node-rows',
        rule='indexed-work',
        budget='plan-work',
        scope=(
                'Two current cardinalities for cases declaring expected_indexes. Node-row sums '
                'count work across plan levels; they are not unique output rows. Two fixture '
                'points are a regression check, not an asymptotic proof.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_compare_cardinalities',
        ),
    ),
    Objective(
        id='database-unindexed-work-growth',
        metric='sum of plan Actual Rows x Actual Loops',
        unit='plan-node-rows',
        rule='linear-work',
        budget='plan-work',
        scope=(
                'Two current cardinalities for cases without expected_indexes. Uses actual '
                'relation-row growth; same caveats as the indexed-work check.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_compare_cardinalities',
        ),
    ),
    Objective(
        id='database-plan-latency-growth',
        metric='PostgreSQL EXPLAIN ANALYZE execution time',
        unit='milliseconds',
        rule='plan-latency',
        budget='plan-latency',
        scope=(
                'Paired cardinalities, identical plan case. Retained broad '
                'runner/cache-sensitive regression envelope; not a user-facing latency target.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_compare_cardinalities',
        ),
    ),
    Objective(
        id='database-stream-memory-growth',
        metric='peak traced Python allocations during SQL stream',
        unit='bytes',
        rule='memory-growth',
        budget='memory-growth',
        scope=(
                'Paired cardinalities, identical statement case. Relative envelope supplements '
                'the absolute stream ceiling.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_compare_cardinalities',
        ),
    ),
    Objective(
        id='database-stream-latency-growth',
        metric='full stream wall time / max(returned rows, 1)',
        unit='milliseconds/row',
        rule='stream-latency',
        budget='stream-latency',
        scope=(
                'Paired stream cases; denominator max(returned rows, 1). Empty results '
                'represent empty-query cost rather than per-row observations. Full-stream wall '
                'time includes cursor/transaction cleanup, not time to first byte or end-user '
                'request latency.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_compare_cardinalities',
        ),
    ),
    Objective(
        id='database-http-memory-growth',
        metric='peak traced Python allocations during client traversal',
        unit='bytes',
        rule='memory-growth',
        budget='memory-growth',
        scope=(
                'Paired cardinalities for each of the three named HTTP traversal paths; no '
                'averaging between paths.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_compare_cardinalities',
        ),
    ),
    Objective(
        id='database-plan-temp-io',
        metric='sum of reported plan-node temporary reads and writes',
        unit='blocks',
        rule='maximum',
        budget='plan-temp-io',
        scope=(
                'Current bounded selector-plan fixtures should not spill to temporary storage. '
                'The node sum is used only to test zero, not to estimate unique physical I/O.'
        ),
        existing_use='existing-check',
        sources=(
            'scripts/database_qualification.py::_measure_plan',
        ),
    ),
    Objective(
        id='listener-observation-peak',
        metric='peak traced Python allocations during observe',
        unit='bytes',
        rule='exclusive-maximum',
        budget='listener-observation',
        scope=(
                'One current unmarked mount after seeding 50000 historical mounts; seeding is '
                'outside tracing. The ceiling is fixture-local and does not by itself prove '
                'history-independent complexity.'
        ),
        existing_use='existing-check',
        sources=(
            (
                'reference/gogurt/application/tests/test_listener.py::test_listener_observation'
                '_memory_depends_on_current_mounts_not_history'
            ),
        ),
    ),
    Objective(
        id='listener-runnable-peak',
        metric='peak traced Python allocations during runnable selection',
        unit='bytes',
        rule='exclusive-maximum',
        budget='listener-runnable',
        scope=(
                'Select three runnable dispatches from a fixture of 10000 queued rows. Seeding '
                'is outside tracing.'
        ),
        existing_use='existing-check',
        sources=(
            (
                'reference/gogurt/application/tests/test_listener.py::test_listener_runnable_se'
                'lection_is_bounded_by_available_custody'
            ),
        ),
    ),
)

# Accounting dispositions keep measurements and correctness/extent controls from
# being mistaken for missing numerical targets. They never create thresholds.
DISPOSITIONS = (
    ("observation-only", "reference-recovery", "scripts/transfer_profile.py::main",
     'Three workload labels; record completed bytes, items and elapsed time. No '
         'existing rate target; do not manufacture one.'),
    ("observation-only", "process-cold-cli-startup",
        "scripts/operation_qualification.py::_cold_cli_timings",
     'Three fresh Python processes per entrypoint by default; --help elapsed '
         'min/median/max and output identity. '
     "Not a cold OS filesystem cache; no existing latency ceiling."),
    ("observation-only", "local-api-and-client-timings",
        "scripts/operation_qualification.py::_load_operation_timings",
     'Successful local-fixture server/client wall-time summaries joined to the '
         'executable operation matrix. '
     'Coverage/positive-sample requirements are not speed targets; no invented '
         'per-endpoint budget.'),
    ("observation-only", "stove0-scale", "scripts/test_compose_smoke.sh",
     'Completion elapsed time, file counts, database/document bytes, target CPU '
         'and cgroup peak memory. '
     '128 files is a fixture size, not a capacity guarantee. Cgroup counters '
         'cover their actual lifetime, not necessarily just the workload.'),
    ("observation-only", "transfer-phase-telemetry", "riverhog/src/riverhog_core/throughput.py",
     'Phase timers, bytes and bottleneck labels are diagnostics. Summed '
         'phase/segment work can overlap or count retries; '
     'never use those sums as unique end-to-end goodput or exclusive '
         'elapsed-time attribution.'),
    ("observation-only", "database-first-row-and-planning-time", _DB,
     'Planning time, first-item time and absolute traversal durations are '
         'retained observations; only the named objectives above carry bounds.'),
    ("not-an-objective", "extent-and-structural-correctness", "scripts/extent_contract.py",
     'Bounded pages, no truncation, exact results, row/query counts, '
         'continuation, natural-plan shape and cleanup assertions '
     'remain with their present owners. This inventory does not weaken them or '
         'reclassify contracts.'),
    ("not-an-objective", "runtime-capacity-and-estimates",
        "riverhog/src/riverhog_core/throughput.py",
     'Concurrency/chunk/memory settings, quotas and retrieval estimated latency '
         'configure operation. '
     "They are neither measured capacity baselines nor performance commitments."),
    ("not-an-objective", "harness-deadlines-and-cleanup", "scripts/test_compose_smoke.sh",
     'Wait/timeout limits bound the test harness. Empty temporary workspace '
         'after completion is cleanup correctness, '
