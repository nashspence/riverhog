"""Indexed durable retirement of exact native validation jobs and private files."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
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
from riverhog_provenance import JournalValidationCapacity, reclaim_retired_journal_workspace
from sqlalchemy import delete, or_, select, text, update
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.growth_bounds import bound_envelope
from riverhog_core.journal_validation_retirement import (
    lease_retiring_journal_workspace,
    remove_retired_journal_workspace_directory,
)
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace

from . import upload_journal_validation_jobs as jobs


def _retirement_catalog_declaration(mode: str) -> Declaration:
    selecting = mode == "_select"
    statements, contexts, versions = (6, 6, 0) if selecting else (1, 1, 1)

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def octets(value: int) -> Expression:
        return Expression("constant", value=value, unit=OCTETS)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    return Declaration(
        id="riverhog-server.validation-retirement-catalog-" + mode.strip("_") + ".growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_validation_retirement.UploadJournalValidationRetirement."
            + mode,
        ),
        scope="exact indexed retired-job " + mode.strip("_") + " with complete native catalog work",
        starts_at=(
            "receive exact scheduling cursors/exclusions or a pinned detached "
            "job and proposed controls"
        ),
        ends_at=(
            "close every owned catalog context and return the exact selected "
            "record or mutation outcome"
        ),
        dimensions=(
            Dimension(
                "retained_rows", COUNT, ("Independent retained Job and source Journal index rows.")
            ),
            Dimension(
                "metadata_row_octets",
                OCTETS,
                (
                    "Largest complete actual job/key row "
                    "and proposed update, retaining every "
                    "opaque binding, progress, outcome and"
                    " failure width."
                ),
            ),
            Dimension(
                "control_octets",
                OCTETS,
                (
                    "Complete encoded input job, cursors, "
                    "exclusions and proposed state/failure "
                    "controls."
                ),
            ),
            Dimension(
                "excluded_jobs",
                COUNT,
                (
                    "Actual identities excluded by this exact "
                    "selection call; zero for point mutations."
                ),
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(statements),
                (
                    "Selection probes "
                    "three phases with at"
                    " most one keyset "
                    "probe and one wrap "
                    "per phase; mutations"
                    " use one exact point"
                    " CAS."
                ),
            ),
            Bound(
                "driver_statements",
                COUNT,
                "upper",
                count(24 * contexts),
                (
                    "Include actual cold "
                    "pool/schema setup "
                    "and each owned "
                    "native "
                    "snapshot/transaction"
                    " context."
                ),
            ),
            Bound(
                "connection_statements",
                COUNT,
                "upper",
                count(20 * contexts),
                ("Driver control statements are observed separately from application queries."),
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(12 * contexts),
                (
                    "Actual fetched job "
                    "rows and "
                    "connection-control "
                    "results remain "
                    "observed; selection "
                    "returns at most one "
                    "application job."
                ),
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                scale(12 * contexts, dimension("metadata_row_octets")),
                (
                    "Returned fields "
                    "retain complete "
                    "actual control "
                    "widths; no payload "
                    "is classified as a "
                    "short key."
                ),
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                add(
                    scale(32 * contexts, dimension("metadata_row_octets")),
                    scale(4 * contexts, dimension("control_octets")),
                ),
                (
                    "Actual "
                    "keyset/exclusion "
                    "names, old "
                    "source/worker/version"
                    " fences and proposed"
                    " state or repeated "
                    "failure text are "
                    "retained."
                ),
            ),
            Bound(
                "changed_rows",
                COUNT,
                "equal" if selecting else "upper",
                count(0 if selecting else 1),
                (
                    "Selection changes no"
                    " durable state; a "
                    "unique pinned orphan"
                    " row bounds a "
                    "transition or newly "
                    "recorded blocker."
                ),
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                octets(0),
                ("No source, capture, documentary, schema or native database payload is fetched."),
            ),
            *(
                Bound(
                    metric,
                    unit,
                    "equal",
                    octets(0) if unit == OCTETS else count(0),
                    (
                        "Retirement controls "
                        "remain opaque and "
                        "are never decoded, "
                        "reconstructed or "
                        "re-encoded."
                    ),
                )
                for metric, unit in (
                    ("json_decodes", COUNT),
                    ("json_encodes", COUNT),
                    ("json_decoded_octets", OCTETS),
                    ("json_encoded_octets", OCTETS),
                )
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "equal",
                octets(16 * versions),
                (
                    "A point mutation "
                    "proposal hashes one "
                    "actual UUID for its "
                    "CAS version, "
                    "including a "
                    "stale/no-change "
                    "proposal."
                ),
            ),
            Bound(
                "uuid_values",
                COUNT,
                "equal",
                count(versions),
                (
                    "Selection creates no "
                    "identity; each point "
                    "proposal creates exactly"
                    " one version identity."
                ),
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                add(
                    count(8192 * contexts),
                    scale(
                        2048 * contexts,
                        Expression(
                            "product",
                            terms=(
                                add(count(1), dimension("excluded_jobs")),
                                Expression(
                                    "log2_ceil", terms=(add(count(2), dimension("retained_rows")),)
                                ),
                            ),
                        ),
                    ),
                ),
                (
                    "Maintained indexed phase/keyset/point work includes all actual "
                    "driver instructions and visits dominated by the excluded "
                    "candidate count."
                ),
            ),
        ),
        conditions=(
            (
                "The maintained SQLite workspace/source keys, state/workspace "
                "index and exact orphan partial-index predicate are present."
            ),
            (
                "The owner factory disables automatic flushing; unrelated pending "
                "mutations do not enter these contexts."
            ),
            (
                "Scheduling cursors and exclusions belong to the current candidate"
                " loop; all actual input widths remain independent of observed "
                "work."
            ),
            (
                "State transitions use the owning irreversible retiring/directory "
                "phases. Opaque retired controls need not be reparsed to grant "
                "cleanup authority."
            ),
        ),
        invariants=(
            (
                "Selection preserves every database row and advances only its "
                "in-memory phase/keyset cursor after selecting a real orphan."
            ),
            (
                "Linked sources and stale job/source/worker/version/state "
                "identities cannot be changed by retirement proposals."
            ),
            "An identical blocker does not rewrite its job or create a durable hot retry loop.",
            (
                "Confirmed transitions clear the previous blocker; exact source "
                "bytes and archive authority remain outside these disposable "
                "catalog operations."
            ),
            (
                "Owned catalog contexts close before detached results return; no "
                "database census replaces indexed selection."
            ),
        ),
        limitations=(
            (
                "Logical metadata/SQL/hash/VM observations exclude B-tree byte "
                "comparisons, allocation, physical I/O and lock or CPU latency."
            ),
            (
                "Kernel custody, native extent disposal, final directory/catalog "
                "deletion and whole retirement scheduling retain separate composed"
                " evidence."
            ),
        ),
    )


def _retirement_unit_children() -> tuple[tuple[str, Callable[..., Any]], ...]:
    return (
        ("transition", UploadJournalValidationRetirement._transition),
        ("lease", lease_retiring_journal_workspace),
        ("reclaim", reclaim_retired_journal_workspace),
        ("directory", remove_retired_journal_workspace_directory),
    )


def _retirement_unit_declaration() -> Declaration:
    children = {tag: growth_binding(operation) for tag, operation in _retirement_unit_children()}

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def constant(value: int, unit: tuple[tuple[str, int], ...] = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def add(*terms: Expression) -> Expression:
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def times(calls: Expression, term: Expression) -> Expression:
        return Expression("product", terms=(constant(1, (("count", -1),)), calls, term))

    aliases = {
        "metadata_calls": "stat_calls",
        "owner_identity_calls": "uid_calls",
        "sync_calls": "fsync_calls",
        "payload_read_octets": "payload_octets",
        "enumeration_calls": "directory_enumerations",
    }
    units: dict[str, tuple[tuple[str, int], ...]] = {}
    for binding in children.values():
        for bound in binding.declaration.bounds:
            metric = aliases.get(bound.metric, bound.metric)
            if metric in units and units[metric] != bound.unit:
                raise ValueError("retirement primitive aliases require identical units")
            units[metric] = bound.unit

    def child(tag: str, bound: Bound) -> Expression:
        return Expression(
            "delegate",
            name=tag,
            metric=bound.metric,
            terms=(dimension(tag + "_calls"),),
            arguments=tuple(
                (item.name, dimension(tag + "_" + item.name))
                for item in children[tag].declaration.dimensions
            ),
        )

    # The exact catalog DELETE is owned here. It has no version proposal or codec.
    deletion = {
        "sql_statements": constant(1),
        "driver_statements": constant(24),
        "connection_statements": constant(20),
        "returned_rows": constant(12),
        "returned_metadata_octets": scale(12, dimension("metadata_row_octets")),
        "bound_parameter_octets": add(
            scale(32, dimension("metadata_row_octets")), scale(4, dimension("control_octets"))
        ),
        "changed_rows": constant(1),
        "sqlite_vm_instructions": add(
            constant(8192),
            Expression(
                "product",
                terms=(
                    constant(2048),
                    Expression("log2_ceil", terms=(add(constant(2), dimension("retained_rows")),)),
                ),
            ),
        ),
    }
    own = {metric: times(dimension("delete_calls"), value) for metric, value in deletion.items()}
    # Native extent disposal counts fixed syscalls; include its full path arguments here.
    own["path_octets"] = times(
        dimension("reclaim_calls"),
        add(
            dimension("root_path_octets"),
            dimension("workspace_identity_octets"),
            constant(513, OCTETS),
        ),
    )
    return Declaration(
        id="riverhog-server.validation-retirement-unit.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_validation_retirement.UploadJournalValidationRetirement._advance",
        ),
        scope=(
            "one exact retired-job unit including catalog, full custody "
            "contexts, byte disposal and directory completion"
        ),
        starts_at="receive the detached pinned job and explicit positive file-extent admission",
        ends_at=(
            "close every catalog/kernel resource and return the progress "
            "decision or propagate the real storage rejection"
        ),
        dimensions=(
            Dimension(
                "octet_budget",
                OCTETS,
                ("Positive caller admission for this complete retirement unit."),
            ),
            Dimension(
                "retained_rows",
                COUNT,
                (
                    "Independent retained Job and Journal index "
                    "population before the exact final catalog "
                    "deletion."
                ),
            ),
            Dimension(
                "metadata_row_octets",
                OCTETS,
                ("Complete actual job/key row and old deletion fence widths."),
            ),
            Dimension(
                "control_octets",
                OCTETS,
                ("Complete detached job and state controls retained in the final deletion fence."),
            ),
            Dimension(
                "root_path_octets", OCTETS, ("Full native encoded owner root supplied at entry.")
            ),
            Dimension(
                "workspace_identity_octets",
                OCTETS,
                ("Full encoded detached workspace identity supplied at entry."),
            ),
            Dimension(
                "delete_calls",
                COUNT,
                ("Actual exact final catalog DELETE invocations, independently capped at one."),
            ),
            *(
                Dimension(
                    tag + "_calls",
                    COUNT,
                    (
                        "Actual child invocations, independently "
                        "capped by the single selected state branch."
                    ),
                )
                for tag in children
            ),
            *(
                Dimension(
                    tag + "_" + item.name,
                    item.unit,
                    (
                        "Exact selected child input, "
                        "including rejected work; zero is"
                        " allowed only for an absent "
                        "child. "
                    )
                    + item.meaning,
                )
                for tag, binding in children.items()
                for item in binding.declaration.dimensions
            ),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    unit,
                    "upper",
                    add(
                        *(
                            child(tag, bound)
                            for tag, binding in children.items()
                            for bound in binding.declaration.bounds
                            if aliases.get(bound.metric, bound.metric) == metric
                        ),
                        own.get(metric, constant(0, unit)),
                    ),
                    (
                        "Complete observed "
                        "child work plus the "
                        "exact owned catalog "
                        "deletion and native "
                        "path arguments; "
                        "aliases preserve "
                        "primitive units."
                    ),
                )
                for metric, unit in units.items()
            ),
            *(
                Bound(
                    tag + "_calls",
                    COUNT,
                    "equal",
                    dimension(tag + "_calls"),
                    ("Every invocation retains its independently validated real child sample."),
                )
                for tag in children
            ),
            *(
                Bound(
                    tag + "_calls_limit",
                    COUNT,
                    "upper",
                    constant(1),
                    ("The selected state can enter each child at most once."),
                )
                for tag in children
            ),
            Bound(
                "delete_calls",
                COUNT,
                "equal",
                dimension("delete_calls"),
                ("Actual final catalog deletion attempts are observed separately."),
            ),
            Bound(
                "delete_calls_limit",
                COUNT,
                "upper",
                constant(1),
                ("One confirmed directory result permits at most one exact catalog deletion."),
            ),
            Bound(
                "native_admission_octets",
                OCTETS,
                "upper",
                dimension("octet_budget"),
                ("The actual native child receives no more than the caller admission."),
            ),
            Bound(
                "reclaim_requires_lease",
                COUNT,
                "upper",
                dimension("lease_calls"),
                ("No native file extent is touched outside its selected exclusive lease."),
            ),
            Bound(
                "custody_branches",
                COUNT,
                "upper",
                constant(1),
                ("Lease and final directory cleanup are mutually exclusive state branches."),
            ),
            Bound(
                "catalog_branches",
                COUNT,
                "upper",
                constant(1),
                ("A state transition and final deletion are mutually exclusive state branches."),
            ),
            *(
                Bound(
                    "own_" + metric,
                    units[metric],
                    "upper",
                    own[metric],
                    (
                        "The exact catalog "
                        "deletion has one "
                        "indexed transaction "
                        "and retains complete"
                        " actual control "
                        "widths."
                    ),
                )
                for metric in deletion
            ),
            *(
                Bound(
                    tag + "_" + bound.metric,
                    bound.unit,
                    "upper",
                    child(tag, bound),
                    (
                        "Retain the complete "
                        "actual child "
                        "primitive "
                        "observation and its "
                        "source-owned bound."
                    ),
                )
                for tag, binding in children.items()
                for bound in binding.declaration.bounds
            ),
        ),
        conditions=(
            (
                "The catalog factory disables autoflush and preserves maintained "
                "state/source/workspace indexes."
            ),
            (
                "The job is detached and pinned; supplied child dimensions "
                "describe the actual selected inputs rather than observed costs."
            ),
            (
                "The file-extent admission is a positive integer; native storage "
                "retains the owning POSIX custody contract."
            ),
        ),
        invariants=(
            "Linked sources are rejected before catalog or filesystem work.",
            (
                "Retirement is durable before extent disposal, and directory phase"
                " is durable before lease/directory removal."
            ),
            (
                "Every byte-disposal unit holds the exact exclusive workspace "
                "lease and excludes all independent result readers."
            ),
            (
                "Missing directories reconcile through the durable directory "
                "phase; unclassified files are preserved."
            ),
            (
                "A final catalog deletion retains source/worker/version/state "
                "fences even after directory removal."
            ),
            (
                "Real capacity, custody and interruption outcomes retain their "
                "actual partial extent work and close acquired resources."
            ),
        ),
        limitations=(
            (
                "Logical SQL, metadata, filesystem and extent observations exclude"
                " physical allocation, internal index/path comparisons, I/O and "
                "lock/CPU latency."
            ),
            (
                "The outer scheduling loop retains its own selection, candidate, "
                "failure-formatting and repeated-unit composition."
            ),
        ),
    )


def _retirement_loop_children() -> tuple[tuple[str, Callable[..., Any]], ...]:
    return (
        ("select", UploadJournalValidationRetirement._select),
        ("unit", UploadJournalValidationRetirement._advance),
        ("blocked", UploadJournalValidationRetirement._blocked),
    )


def _retirement_loop_declaration() -> Declaration:
    children = {tag: growth_binding(operation) for tag, operation in _retirement_loop_children()}

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def times(calls: Expression, term: Expression) -> Expression:
        return Expression(
            "product", terms=(Expression("constant", value=1, unit=(("count", -1),)), calls, term)
        )

    def add(terms: tuple[Expression, ...], unit: tuple[tuple[str, int], ...]) -> Expression:
        if not terms:
            return Expression("constant", value=0, unit=unit)
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    units: dict[str, tuple[tuple[str, int], ...]] = {}
    for binding in children.values():
        for bound in binding.declaration.bounds:
            if bound.metric in units and units[bound.metric] != bound.unit:
                raise ValueError("retirement loop primitive units must agree")
            units[bound.metric] = bound.unit

    def child(tag: str, metric: str, unit: tuple[tuple[str, int], ...]) -> Expression:
        binding = children[tag]
        if metric not in {bound.metric for bound in binding.declaration.bounds}:
            return Expression("constant", value=0, unit=unit)
        arguments = {
            item.name: dimension(tag + "_" + item.name) for item in binding.declaration.dimensions
        }
        return times(
            dimension(tag + "_calls"), bound_envelope(binding.operation, metric, arguments)
        )

    limits = {
        "select": scale(3, dimension("unit_limit")),
        "unit": dimension("select_calls"),
        "blocked": dimension("unit_calls"),
    }
    return Declaration(
        id="riverhog-server.validation-retirement-loop.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_validation_retirement.UploadJournalValidationRetirement.advance",
        ),
        scope=(
            "complete bounded retirement scheduling with real heterogeneous catalog and file units"
        ),
        starts_at="receive the finite unit limit and positive per-unit file-extent admission",
        ends_at=(
            "return actual progressed units after bounded candidates and real "
            "blocker recording, or propagate a rejected control call"
        ),
        dimensions=(
            Dimension(
                "unit_limit",
                COUNT,
                ("Nonnegative effective caller unit limit, independent of observed progress."),
            ),
            Dimension(
                "octet_budget", OCTETS, ("Positive caller admission for each attempted file unit.")
            ),
            Dimension(
                "error_control_octets",
                OCTETS,
                (
                    "Largest complete received "
                    "capacity/custody error argument "
                    "before bounded blocker slicing."
                ),
            ),
            *(
                Dimension(
                    tag + "_calls",
                    COUNT,
                    (
                        "Actual child invocations, independently "
                        "capped by the finite scheduling loop."
                    ),
                )
                for tag in children
            ),
            *(
                Dimension(
                    tag + "_" + item.name,
                    item.unit,
                    (
                        "Dominating envelope over the "
                        "actual selected child inputs; "
                        "absent children may use zero. "
                    )
                    + item.meaning,
                )
                for tag, binding in children.items()
                for item in binding.declaration.dimensions
            ),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    unit,
                    "upper",
                    add(tuple(child(tag, metric, unit) for tag in children), unit),
                    (
                        "Complete actual "
                        "child work bounded "
                        "by monotone "
                        "source-owned numeric"
                        " envelopes; each "
                        "invocation retains "
                        "its own "
                        "independently "
                        "validated inputs."
                    ),
                )
                for metric, unit in units.items()
            ),
            *(
                Bound(
                    tag + "_calls",
                    COUNT,
                    "equal",
                    dimension(tag + "_calls"),
                    ("Every selected child invocation retains its actual sample."),
                )
                for tag in children
            ),
            *(
                Bound(
                    tag + "_calls_limit",
                    COUNT,
                    "upper",
                    limit,
                    (
                        "Each outer unit attempts "
                        "at most three candidates;"
                        " only selected jobs "
                        "advance and only real "
                        "unit failures record "
                        "blockers."
                    ),
                )
                for tag, limit in limits.items()
            ),
            Bound(
                "outer_iterations",
                COUNT,
                "upper",
                dimension("unit_limit"),
                ("The real outer range never exceeds the caller limit."),
            ),
            Bound(
                "candidate_iterations",
                COUNT,
                "upper",
                scale(3, dimension("unit_limit")),
                (
                    "The actual candidate"
                    " loop performs at "
                    "most three "
                    "iterations for each "
                    "outer unit."
                ),
            ),
            Bound(
                "excluded_jobs_per_call",
                COUNT,
                "upper",
                count(2),
                (
                    "The current per-unit"
                    " candidate list "
                    "contains at most two"
                    " earlier identities "
                    "before a selection "
                    "call."
                ),
            ),
            Bound(
                "exclusion_copy_references",
                COUNT,
                "upper",
                scale(6, dimension("unit_limit")),
                (
                    "Actual "
                    "excluded-identity "
                    "tuples retain at "
                    "most two references "
                    "per candidate call."
                ),
            ),
            Bound(
                "error_format_calls",
                COUNT,
                "upper",
                dimension("unit_calls"),
                ("Each actual capacity/custody failure is formatted once."),
            ),
            Bound(
                "error_formatted_octets",
                OCTETS,
                "upper",
                times(dimension("unit_calls"), dimension("error_control_octets")),
                (
                    "Complete actual "
                    "error formatting "
                    "remains charged "
                    "before the bounded "
                    "detached blocker "
                    "slice."
                ),
            ),
            Bound(
                "native_admission_per_call",
                OCTETS,
                "upper",
                dimension("octet_budget"),
                ("Each selected file unit receives at most the caller admission."),
            ),
            Bound(
                "progressed_units",
                COUNT,
                "upper",
                dimension("unit_limit"),
                ("Confirmed file/catalog/blocker progress is counted at most once per outer unit."),
            ),
            *(
                Bound(
                    tag + "_" + bound.metric,
                    bound.unit,
                    "upper",
                    child(tag, bound.metric, bound.unit),
                    (
                        "Preserve the actual "
                        "aggregate child "
                        "metric without "
                        "requiring "
                        "heterogeneous calls "
                        "to have identical "
                        "dimensions."
                    ),
                )
                for tag, binding in children.items()
                for bound in binding.declaration.bounds
            ),
        ),
        conditions=(
            (
                "The unit limit is a finite integer; the exact positive integer "
                "octet admission is validated before work."
            ),
            (
                "Actual child dimensions are independently obtained from their "
                "selected inputs; componentwise envelopes dominate them and use "
                "monotone nonnegative numeric bounds."
            ),
            (
                "Catalog indexes, detached fences and exclusive private workspace "
                "custody retain their child contracts."
            ),
        ),
        invariants=(
            (
                "A busy or blocked job does not prevent another candidate in the "
                "same bounded attempt from progressing."
            ),
            (
                "The exact real error is formatted and recorded without "
                "fabricating successful validation or replacing source/archive "
                "authority."
            ),
            (
                "An unchanged blocker does not rewrite its row; exhausted "
                "candidates stop without a hot retry loop."
            ),
            (
                "Durable phases, exact final deletion fences and independent "
                "reader exclusion remain with every actual child invocation."
            ),
            "No unscoped catalog/filesystem work or archive-size census is hidden in coordination.",
        ),
        limitations=(
            (
                "Logical loop, reference, SQL, control, file and extent "
                "observations exclude physical allocation, internal index/path "
                "comparisons, I/O and lock/CPU latency."
            ),
            (
                "This finite scheduling call does not replace whole "
                "service/pipeline composition or member qualification."
            ),
        ),
    )


_PHASES = ("directory", "retiring", "orphan")


class UploadJournalValidationRetirement:
    def __init__(self, factory: SessionFactory, root: Path) -> None:
        self.factory, self.root = factory, root
        self._after: dict[str, str | None] = dict.fromkeys(_PHASES)
        self._phase = 0

    def _select(self, excluding: tuple[str, ...]) -> jobs.ValidationJob | None:
        for distance in range(len(_PHASES)):
            index = (self._phase + distance) % len(_PHASES)
            phase = _PHASES[index]
            query = select(Job).where(Job.collection_id.is_(None), Job.workspace_id > "")
            if phase == "orphan":
                query = query.where(text("state NOT IN ('retiring','directory')"))
            else:
                query = query.where(Job.state == phase)
            if excluding:
                query = query.where(Job.workspace_id.not_in(excluding))
            query = query.order_by(Job.workspace_id).limit(1)
            with read_snapshot(self.factory) as session:
                row = None
                after = self._after[phase]
                if after is not None:
                    row = session.scalar(query.where(Job.workspace_id > after))
                if row is None:
                    row = session.scalar(query)
                selected = None if row is None else jobs.ValidationJob.read(row)
            if selected is not None:
                self._after[phase] = selected.workspace_id
                self._phase = (index + 1) % len(_PHASES)
                return selected
        return None

    def _transition(self, job: jobs.ValidationJob, state: str) -> jobs.ValidationJob | None:
        with self.factory() as session, session.begin():
            changed = session.execute(
                update(Job)
                .where(
                    *jobs._identity(job),
                    Job.version == job.version,
                    Job.state == job.state,
                    Job.collection_id.is_(None),
                    Job.journal_id.is_(None),
                )
                .values(state=state, version=jobs._version(), failure=None)
                .returning(Job)
            ).scalar_one_or_none()
            return None if changed is None else jobs.ValidationJob.read(changed)

    def _blocked(self, job: jobs.ValidationJob, failure: str) -> bool:
        with self.factory() as session, session.begin():
            changed = session.execute(
                update(Job)
                .where(
                    *jobs._identity(job),
                    Job.version == job.version,
                    Job.state == job.state,
                    Job.collection_id.is_(None),
                    or_(Job.failure.is_(None), Job.failure != failure),
                )
                .values(version=jobs._version(), failure=failure)
                .returning(Job.workspace_id)
            ).scalar_one_or_none()
            return changed is not None

    def _advance(self, job: jobs.ValidationJob, octet_budget: int) -> bool:
        if job.collection_id is not None or job.journal_id is not None:
            raise RuntimeError("native cleanup cannot retire a linked source")
        if job.state not in {"retiring", "directory"}:
            selected = self._transition(job, "retiring")
            if selected is None:
                return False
            # Retirement is durable before any data extent can be removed.
            return True
        if job.state == "directory":
            if not remove_retired_journal_workspace_directory(self.root, job.workspace_id):
                return False
            with self.factory() as session, session.begin():
                removed = session.execute(
                    delete(Job)
                    .where(
                        *jobs._identity(job),
                        Job.version == job.version,
                        Job.state == "directory",
                        Job.collection_id.is_(None),
                    )
                    .returning(Job.workspace_id)
                ).scalar_one_or_none()
                return removed is not None
        with lease_retiring_journal_workspace(self.root, job.workspace_id) as owned:
            if owned is None:
                return False
            if owned.directory is None:
                return self._transition(job, "directory") is not None
            progress = reclaim_retired_journal_workspace(owned.directory, octet_budget=octet_budget)
            if progress.files_reclaimed:
                return self._transition(job, "directory") is not None
            return progress.removed_octets > 0 or progress.removed_files > 0

    def advance(self, *, limit: int = 1, octet_budget: int = 4 * 1024 * 1024) -> int:
        if type(octet_budget) is not int or octet_budget < 1:
            raise ValueError("retirement requires a positive file-extent admission")
        processed = 0
        for _ in range(max(0, limit)):
            attempted: list[str] = []
            progressed = False
            for _candidate in range(3):
                job = self._select(tuple(attempted))
                if job is None:
                    break
                attempted.append(job.workspace_id)
                try:
                    progressed = self._advance(job, octet_budget)
                except (JournalValidationCapacity, LostJournalValidationWorkspace) as error:
                    progressed = self._blocked(job, str(error)[:1000])
                if progressed:
                    processed += 1
                    break
            if not progressed:
                break
        return processed


for _catalog_operation in ("_select", "_transition", "_blocked"):
    declare_growth(_retirement_catalog_declaration(_catalog_operation))(
        getattr(UploadJournalValidationRetirement, _catalog_operation)
    )
del _catalog_operation


declare_growth(_retirement_unit_declaration(), delegates=_retirement_unit_children())(
    UploadJournalValidationRetirement._advance
)


declare_growth(_retirement_loop_declaration(), delegates=_retirement_loop_children())(
    UploadJournalValidationRetirement.advance
)
