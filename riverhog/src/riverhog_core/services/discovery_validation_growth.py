"""Composed lifecycle costs for the application discovery owner."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    growth_binding,
)
from riverhog_provenance import JournalValidationSession, RetainedCorpusValidator

from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_corpus_source import DiscoveryCorpusSource
from riverhog_core.canonical_discovery_metadata import DiscoveryMetadata
from riverhog_core.canonical_discovery_selection import frozen_corpus_selection
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_core.journal_validation_workspace_reference import ValidationWorkspaceReference

_COUNT_METRICS = (
    "core_sql_statements",
    "core_returned_rows",
    "core_changed_rows",
    "core_opened_connections",
    "core_closed_connections",
    "core_vm_instructions",
    "catalog_sql_statements",
    "catalog_driver_statements",
    "catalog_connection_statements",
    "catalog_returned_rows",
    "catalog_changed_rows",
    "catalog_vm_instructions",
    "catalog_json_encodes",
    "catalog_json_decodes",
    "uuid_values",
    "native_sql_statements",
    "native_returned_rows",
    "native_changed_rows",
    "native_opened_connections",
    "native_closed_connections",
    "stream_close_calls",
    "native_vm_instructions",
    "native_source_checks",
    "native_opening_callbacks",
    "native_release_callbacks",
    "open_calls",
    "close_calls",
    "stat_calls",
    "uid_calls",
    "mkdir_calls",
    "fsync_calls",
    "flock_calls",
    "creation_callbacks",
    "custody_callbacks",
    "directory_enumerations",
    "source_reads",
    "native_anchor_calls",
    "pool_visits",
)
_OCTET_METRICS = (
    "core_returned_octets",
    "core_parameter_octets",
    "catalog_returned_octets",
    "catalog_parameter_octets",
    "catalog_json_encoded_octets",
    "catalog_json_decoded_octets",
    "native_returned_octets",
    "native_parameter_octets",
    "encoded_octets",
    "decoded_octets",
    "hashed_octets",
    "sha256_block_octets",
    "written_octets",
    "encoded_path_octets",
    "source_payload_octets",
    "pool_result_octets",
)


def _count(value: int) -> Expression:
    return Expression("constant", value=value, unit=COUNT)


def _dimension(name: str) -> Expression:
    return Expression("dimension", name=name)


def _scaled(value: int, term: Expression) -> Expression:
    return Expression("product", terms=(Expression("constant", value=value), term))


def _slot_calls() -> Expression:
    return Expression(
        "product",
        terms=(
            _dimension("active_slots"),
            Expression("constant", value=1, unit=(("count", -1),)),
        ),
    )


def _sum(terms: tuple[Expression, ...], unit: tuple[tuple[str, int], ...]) -> Expression:
    if not terms:
        return Expression("constant", value=0, unit=unit)
    return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)


def _metric(kind: str, metric: str) -> str:
    if kind == "catalog":
        return {
            "sql_statements": "catalog_sql_statements",
            "driver_statements": "catalog_driver_statements",
            "connection_statements": "catalog_connection_statements",
            "returned_rows": "catalog_returned_rows",
            "returned_metadata_octets": "catalog_returned_octets",
            "bound_parameter_octets": "catalog_parameter_octets",
            "changed_rows": "catalog_changed_rows",
            "sqlite_vm_instructions": "catalog_vm_instructions",
            "json_encodes": "catalog_json_encodes",
            "json_decodes": "catalog_json_decodes",
            "json_encoded_octets": "catalog_json_encoded_octets",
            "json_decoded_octets": "catalog_json_decoded_octets",
        }.get(metric, metric)
    if kind == "input":
        return {
            "sql_statements": "core_sql_statements",
            "returned_rows": "core_returned_rows",
            "returned_octets": "core_returned_octets",
            "parameter_octets": "core_parameter_octets",
            "sqlite_vm_instructions": "core_vm_instructions",
            "source_read_octets": "source_payload_octets",
        }.get(metric, metric)
    if kind == "kernel":
        return {
            "path_octets": "encoded_path_octets",
            "payload_octets": "source_payload_octets",
        }.get(metric, metric)
    if kind == "reference":
        return {"stream_closes": "stream_close_calls"}.get(metric, metric)
    if kind == "journal":
        return {
            "sql_statements": "native_sql_statements",
            "returned_rows": "native_returned_rows",
            "changed_rows": "native_changed_rows",
            "returned_metadata_octets": "native_returned_octets",
            "bound_parameter_octets": "native_parameter_octets",
            "opened_databases": "native_opened_connections",
            "closed_databases": "native_closed_connections",
            "sqlite_vm_instructions": "native_vm_instructions",
            "source_verifications": "native_source_checks",
            "release_callbacks": "native_release_callbacks",
            "retention_callbacks": "native_opening_callbacks",
            "opened_files": "open_calls",
            "closed_files": "close_calls",
            "file_syncs": "fsync_calls",
            "directory_scans": "directory_enumerations",
        }.get(metric, metric)
    if kind == "native":
        return {
            "sql_statements": "native_sql_statements",
            "returned_rows": "native_returned_rows",
            "returned_octets": "native_returned_octets",
            "parameter_octets": "native_parameter_octets",
            "opened_connections": "native_opened_connections",
            "closed_connections": "native_closed_connections",
            "source_checks": "native_source_checks",
            "opening_callbacks": "native_opening_callbacks",
            "release_callbacks": "native_release_callbacks",
            "descriptor_opens": "open_calls",
            "descriptor_closes": "close_calls",
            "file_identity_reads": "stat_calls",
            "checkpoint_encoded_octets": "encoded_octets",
            "checkpoint_decoded_octets": "decoded_octets",
            "source_frame_reads": "source_reads",
            "full_assertion_reads": "source_reads",
        }.get(metric, metric)
    return metric


def _renamed(expression: Expression, kind: str) -> Expression:
    if kind == "journal" and expression.op == "dimension":
        if expression.name in {"source_checks", "release_callbacks"}:
            return _count(int(expression.name == "release_callbacks"))
    names = {
        "metadata_octets": "control_octets",
        "registered_inputs": "retained_core_rows",
        "metadata_row_octets": "control_octets",
        "root_path_octets": "path_octets",
        "workspace_identity_octets": "control_octets",
        "binding_octets": "native_binding_octets",
        "checkpoint_octets": "native_checkpoint_octets",
        "counter_octets": "native_counter_octets",
        "retained_rows": "retained_catalog_rows" if kind == "catalog" else "retained_native_rows",
    }
    return replace(
        expression,
        name=names.get(expression.name, expression.name),
        terms=tuple(_renamed(term, kind) for term in expression.terms),
    )


def _children(mode: str) -> tuple[tuple[str, Callable[..., Any], int], ...]:
    dropped = (
        ("journal", JournalValidationSession.close, 1),
        ("kernel", lease_journal_validation_workspace, 1),
        ("reference", ValidationWorkspaceReference.close, 2),
        ("native", RetainedCorpusValidator.close, 1),
        ("storage", DiscoveryMetadata.close, 1),
        ("kernel", lease_journal_validation_workspace, 1),
    )
    opened = (
        ("catalog", jobs.select_discovery_job, 1),
        ("catalog", jobs.discovery_worker_current, 2),
        ("catalog", jobs.claim_discovery_worker, 1),
        ("catalog", jobs.lose_discovery_corpus, 1),
        ("kernel", lease_journal_validation_workspace, 1),
        ("storage", DiscoveryMetadata.open, 1),
    )
    if mode == "corpus":
        return (
            ("storage", DiscoveryMetadata.read, 1),
            ("input", frozen_corpus_selection, 1),
            ("catalog", jobs.pin_discovery_corpus, 1),
            ("catalog", jobs.discovery_worker_snapshot, 1),
            ("catalog", jobs.activate_discovery_corpus, 1),
            ("catalog", jobs.lose_discovery_corpus, 1),
            ("storage", DiscoveryCorpusSource.__init__, 1),
            ("storage", DiscoveryCorpusSource.verify_current, 2),
            ("native", RetainedCorpusValidator.open_retained, 1),
        )
    if mode == "_open":
        return opened
    if mode == "_drop" or mode == "close":
        return dropped
    if mode == "release_stale":
        return (("storage", DiscoveryMetadata.verify_current, 1), *dropped)
    if mode == "work":
        return (*opened, ("storage", DiscoveryMetadata.verify_current, 2), *dropped)
    return ()


def _declaration(mode: str) -> Declaration:
    sweeping = mode in {"close", "release_stale"}
    children = _children(mode)
    expressions: dict[str, list[Expression]] = {
        metric: [] for metric in (*_COUNT_METRICS, *_OCTET_METRICS)
    }
    for kind, function, calls in children:
        declaration = growth_binding(getattr(function, "__func__", function)).declaration
        for bound in declaration.bounds:
            metric = _metric(kind, bound.metric)
            if metric in expressions:
                expression = _scaled(calls, _renamed(bound.expression, kind))
                if sweeping:
                    expression = Expression("product", terms=(_slot_calls(), expression))
                expressions[metric].append(expression)
                if kind == "storage" and metric == "catalog_driver_statements":
                    expressions["catalog_connection_statements"].append(expression)
    if mode == "corpus":
        # Owner path admission precedes the Native port. Native initialization
        # uses fixed schema/singleton statements on the pinned SQLite backend.
        expressions["stat_calls"].append(_count(1))
        expressions["uid_calls"].append(_count(3))
        expressions["native_changed_rows"].append(_count(3))
        expressions["native_vm_instructions"].append(
            Expression(
                "sum",
                terms=(
                    _count(32768),
                    Expression(
                        "product",
                        terms=(
                            _count(2048),
                            Expression(
                                "log2_ceil",
                                terms=(
                                    Expression(
                                        "sum", terms=(_dimension("retained_native_rows"), _count(2))
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            )
        )
        expressions["encoded_path_octets"].append(_scaled(16, _dimension("path_octets")))
        expressions["encoded_octets"].append(_dimension("native_binding_octets"))
    visits = (
        _scaled(4, _dimension("active_slots"))
        if sweeping
        else _dimension("active_slots")
        if mode == "active_build_ids"
        else _count(8 if mode == "work" else 3 if mode in {"_reserve", "_drop"} else 0)
    )
    expressions["pool_visits"].append(visits)
    if mode == "active_build_ids":
        expressions["pool_result_octets"].append(
            Expression("product", terms=(_slot_calls(), _dimension("control_octets")))
        )
    owner = "DiscoveryValidationWork" if mode == "corpus" else "DiscoveryValidationOwner"
    return Declaration(
        id="riverhog-server.discovery-validation-owner-"
        + mode.strip("_").replace("_", "-")
        + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.services.discovery_validation_owner." + owner + "." + mode,),
        scope="actual catalog, kernel, Core and Native application lifecycle"
        if mode != "work"
        else "entry/exit and exceptional cleanup of a serialized work quantum",
        starts_at="receive original archive authority and independently retained resource counts",
        ends_at="return current owned work or retain uncertain cleanup until closure confirms",
        dimensions=(
            Dimension("retained_core_rows", COUNT, "Independent original Core control/input rows."),
            Dimension("retained_catalog_rows", COUNT, "Independent catalog and job rows."),
            Dimension(
                "retained_native_rows", COUNT, "Independent retained Native input/graph rows."
            ),
            Dimension("active_slots", COUNT, "Occupied slots before and after this operation."),
            Dimension(
                "control_octets", OCTETS, "Maximum complete incoming or stored control width."
            ),
            Dimension("path_octets", OCTETS, "Complete selected root/directory/file path width."),
            Dimension("native_binding_octets", OCTETS, "Complete original Native binding width."),
            Dimension(
                "native_checkpoint_octets", OCTETS, "Complete retained Native checkpoint width."
            ),
            Dimension(
                "native_counter_octets", OCTETS, "Maximum full decimal Native counter width."
            ),
        ),
        bounds=tuple(
            Bound(
                metric,
                unit,
                "upper",
                _sum(tuple(expressions[metric]), unit),
                "Expanded maintained child bounds and fixed owner bookkeeping.",
            )
            for metric, unit in (
                *((metric, COUNT) for metric in _COUNT_METRICS),
                *((metric, OCTETS) for metric in _OCTET_METRICS),
            )
        ),
        conditions=(
            "Work is serialized by the actual slot mutex while the original kernel lease is held.",
            "The work context's body retains separately declared and measured algorithm costs.",
            "The supplied catalog identity is cached; live authorization is checked each time.",
            "Bounds expand maintained storage, source, catalog and kernel lifecycle declarations.",
            "Native initialization VM covers fixed schema/singleton work on the pinned backend.",
            "Complete widths include admitted before/after values, not abbreviated hash labels.",
        ),
        invariants=(
            "Select catalog custody before storage; accepted missing storage is never recreated.",
            "Retain actual handles before initialization or closure can have an uncertain reply.",
            "Remove slots only after Native, Core and original kernel custody have closed.",
            "Pool mutexes do no I/O or Native work and never wait for a slot lock.",
            "Lifecycle checks replay no payload and grant no publication or member-State proof.",
        ),
        limitations=(
            "Context bodies, archive preparation and Native algorithms need their own composition.",
            "VM and logical bytes exclude physical I/O, lock waits and internal byte comparisons.",
            "This lifecycle evidence does not establish a complete collection build or acceptance.",
        ),
    )


def declare_owner_growth(owner: type[Any], work: type[Any]) -> None:
    for mode in (
        "_reserve",
        "_drop",
        "_open",
        "work",
        "active_build_ids",
        "release_stale",
        "close",
    ):
        declare_growth(_declaration(mode))(getattr(owner, mode))
    declare_growth(_declaration("corpus"))(work.corpus)
