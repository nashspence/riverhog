"Bounded live Session ownership with durable worker and source fencing."

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass, field, replace
from pathlib import Path
from threading import Lock
from typing import Any, cast

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    growth_binding,
)
from riverhog_provenance import (
    InvalidJournalGraphCertificate,
    InvalidJournalWorkspace,
    JournalReplayBinding,
    JournalValidationBudget,
    JournalValidationCapacity,
    JournalValidationSession,
    ProvenanceValidationError,
)
from riverhog_provenance_contracts import ContractCatalog

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.growth_bounds import bound_envelope
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    lease_journal_validation_workspace,
)

from . import collection_uploads as source_owner
from . import upload_journal_validation_jobs as jobs
from .upload_journal_certificates import UploadJournalGraphCertificates
from .upload_journal_source import UploadJournalValidationSource

_DEFAULT_BUDGET = JournalValidationBudget()


def _source_fence_children(mode: str) -> tuple[tuple[str, Callable[..., Any]], ...]:
    if mode in {"read_window", "read_exact"}:
        return (
            ("verify", _OwnedSource.verify_current),
            ("read", getattr(UploadJournalValidationSource, mode)),
        )
    if mode == "verify_source":
        return (
            ("worker", jobs.worker_current),
            ("current", UploadJournalValidationSource.verify_current),
        )
    return (
        ("source", _OwnedSource.verify_source),
        ("certificate", UploadJournalGraphCertificates.verify_current),
    )


def _source_fence_declaration(mode: str) -> Declaration:
    children = {
        tag: growth_binding(function).declaration for tag, function in _source_fence_children(mode)
    }

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def zero(unit: tuple[tuple[str, int], ...]) -> Expression:
        return Expression("constant", value=0, unit=unit)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def child(tag: str, metric: str, unit: tuple[tuple[str, int], ...]) -> Expression:
        declaration = children[tag]
        if metric not in {bound.metric for bound in declaration.bounds}:
            return zero(unit)
        return Expression(
            "delegate",
            name=tag,
            metric=metric,
            terms=(dimension(tag + "_calls"),),
            arguments=tuple(
                (item.name, dimension(tag + "_" + item.name)) for item in declaration.dimensions
            ),
        )

    def add(terms: tuple[Expression, ...], unit: tuple[tuple[str, int], ...]) -> Expression:
        if not terms:
            return zero(unit)
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    def total(metric: str, unit: tuple[tuple[str, int], ...]) -> Expression:
        return add(tuple(child(tag, metric, unit) for tag in children), unit)

    # The retained-source point declaration owns its application query. This
    # concrete owner also includes the engine's connection and pool work.
    application_tags = tuple(
        tag
        for tag, declaration in children.items()
        if "driver_statements" not in {bound.metric for bound in declaration.bounds}
    )
    application_queries = add(
        tuple(child(tag, "sql_statements", COUNT) for tag in application_tags), COUNT
    )
    metadata = add(
        tuple(
            child(tag, "returned_metadata_octets", OCTETS)
            if "returned_metadata_octets" in {bound.metric for bound in declaration.bounds}
            else child(tag, "binding_octets", OCTETS)
            for tag, declaration in children.items()
        ),
        OCTETS,
    )

    return Declaration(
        id="riverhog-server.owned-source-" + mode.replace("_", "-") + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.services.upload_journal_validation_owner._OwnedSource." + mode,),
        scope=("concrete worker/source/certificate fencing with complete native connection work"),
        starts_at=(
            "accept the exact selected immutable source, worker and frozen certificate provider"
        ),
        ends_at=(
            "return true or reject stale/inconsistent custody after closing all catalog readers"
        ),
        dimensions=(
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                ("Control-row octets per native maintenance result row over the selected inputs."),
            ),
            *(
                Dimension(
                    tag + "_calls",
                    COUNT,
                    (
                        "Zero or one actual invocation, determined by the pre-call "
                        "fences and earlier rejection order."
                    ),
                )
                for tag in children
            ),
            *(
                Dimension(tag + "_" + item.name, item.unit, item.meaning)
                for tag, declaration in children.items()
                for item in declaration.dimensions
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                total("sql_statements", COUNT),
                ("All application queries belong to the actual source/worker/certificate calls."),
            ),
            Bound(
                "driver_statements",
                COUNT,
                "upper",
                add((total("driver_statements", COUNT), scale(8, application_queries)), COUNT),
                (
                    "Include all declared native calls plus actual source "
                    "application and connection-maintenance statements."
                ),
            ),
            Bound(
                "connection_statements",
                COUNT,
                "upper",
                add((total("connection_statements", COUNT), scale(7, application_queries)), COUNT),
                ("Actual pool checks, cold PRAGMAs and transaction control remain visible."),
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                add((total("returned_rows", COUNT), scale(7, application_queries)), COUNT),
                "Count native control results as well as application rows.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                add(
                    (
                        metadata,
                        Expression(
                            "product",
                            terms=(scale(7, application_queries), dimension("metadata_row_octets")),
                        ),
                    ),
                    OCTETS,
                ),
                (
                    "All returned source/job/certificate controls and "
                    "connection-control results retain their full widths."
                ),
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                total("bound_parameter_octets", OCTETS),
                ("Exact source/job/manifest predicates retain all actual native parameter widths."),
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                add(
                    (total("sqlite_vm_instructions", COUNT), scale(8192, application_queries)),
                    COUNT,
                ),
                (
                    "Actual point-search instructions plus a fixed "
                    "cold-schema/pool envelope for source query contexts; no "
                    "driver work is omitted."
                ),
            ),
            Bound(
                "changed_rows",
                COUNT,
                "equal",
                count(0),
                ("Custody verification never mutates or recreates catalog or native state."),
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                zero(OCTETS),
                "No source/captured/documentary/schema payload is fetched.",
            ),
            *(
                Bound(
                    metric,
                    unit,
                    "upper",
                    total(metric, unit),
                    "Include every actual delegated binding codec or hash operation.",
                )
                for metric, unit in (
                    ("json_decodes", COUNT),
                    ("json_encodes", COUNT),
                    ("json_decoded_octets", OCTETS),
                    ("json_encoded_octets", OCTETS),
                    ("hashed_octets", OCTETS),
                )
            ),
            *(
                Bound(
                    tag + "_calls",
                    COUNT,
                    "equal",
                    dimension(tag + "_calls"),
                    "Observe the exact invocation count.",
                )
                for tag in children
            ),
            *(
                Bound(
                    tag + "_calls_limit",
                    COUNT,
                    "upper",
                    count(1),
                    "A single fence operation invokes each selected child at most once.",
                )
                for tag in children
            ),
            *(
                Bound(
                    tag + "_" + bound.metric,
                    bound.unit,
                    bound.relation,
                    child(tag, bound.metric, bound.unit),
                    "Preserve every actual child metric and its exact input domain.",
                )
                for tag, declaration in children.items()
                for bound in declaration.bounds
            ),
        ),
        conditions=(
            (
                "The maintained SQLite catalog schema, primary keys and "
                "source/worker/manifest indexes are present."
            ),
            (
                "The owning service holds the exact selected workspace "
                "custody while checking its worker; no verification itself "
                "grants a new worker or archive authority."
            ),
            (
                "Source point-query primitives retain their application "
                "scope; all actual pool/connection instructions, statements, "
                "rows and parameters are additionally observed at this "
                "concrete boundary."
            ),
            (
                "Pre-call currentness and binding state determine whether "
                "later children run; invocation counts do not infer or "
                "fabricate accepted native work."
            ),
        ),
        invariants=(
            (
                "The worker is checked before source currentness and the "
                "certificate is checked only after those exact fences pass."
            ),
            (
                "No cached page, known identity or matching prefix bypasses a"
                " required custody fence."
            ),
            (
                "Every snapshot closes on success and rejection, with no "
                "mutation, source replay or missing-state recreation."
            ),
        ),
        limitations=(
            (
                "Logical byte, SQL, codec and VM observations exclude "
                "physical allocation, internal B-tree comparisons, I/O and "
                "lock/CPU latency."
            ),
            (
                "Source reads, native validation, service "
                "coordination/publication and retirement retain their own "
                "composed costs."
            ),
        ),
    )


def _source_read_declaration(mode: str) -> Declaration:
    declaration = _source_fence_declaration(mode)
    read = growth_binding(getattr(UploadJournalValidationSource, mode)).declaration

    def delegated(metric: str) -> Expression:
        return Expression(
            "delegate",
            name="read",
            metric=metric,
            terms=(Expression("dimension", name="read_calls"),),
            arguments=tuple(
                (item.name, Expression("dimension", name="read_" + item.name))
                for item in read.dimensions
            ),
        )

    return replace(
        declaration,
        scope="concrete fenced source reads with complete native connection and payload work",
        starts_at="check worker, source and certificate custody before the actual source read",
        ends_at="return the exact source port result or preserve its currentness/range rejection",
        bounds=(
            *(bound for bound in declaration.bounds if bound.metric != "source_payload_octets"),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    delegated(child_metric),
                    "Actual payload loading, return and assembly retain the source port bound.",
                )
                for metric, child_metric in (
                    ("source_payload_octets", "loaded_payload_octets"),
                    ("loaded_payload_octets", "loaded_payload_octets"),
                    ("returned_octets", "returned_octets"),
                    ("assembled_piece_octets", "assembled_piece_octets"),
                )
            ),
        ),
        invariants=(
            "Worker, source and certificate checks precede the source port's range checks.",
            "The source port repeats currentness before returning even an already cached page.",
            "Short/deferred windows, exact assembly and frame capacity retain their owning rules.",
            "All readers close and no source, job, certificate or native checkpoint is changed.",
        ),
        limitations=(
            "Logical byte, SQL, codec and VM observations exclude physical allocation, internal "
            "B-tree comparisons, I/O and lock/CPU latency.",
            "Source construction, native semantics, service coordination/publication and "
            "retirement retain their own composed costs.",
        ),
    )


class _OwnedSource:
    def __init__(
        self,
        factory: SessionFactory,
        source: UploadJournalValidationSource,
        job: jobs.ValidationJob,
        certificates: UploadJournalGraphCertificates,
    ) -> None:
        self.factory, self.source, self.job = factory, source, job
        self.certificates = certificates

    @property
    def binding(self) -> JournalReplayBinding:
        return self.source.binding

    def verify_source(self) -> bool:
        if not jobs.worker_current(self.factory, self.job) or not self.source.verify_current():
            raise source_owner._StaleUploadJournalValidation
        return True

    def verify_current(self) -> bool:
        self.verify_source()
        if not self.certificates.verify_current():
            raise InvalidJournalGraphCertificate("finite rank preparation changed during replay")
        return True

    def _require_current(self) -> None:
        if not self.verify_current():
            raise source_owner._StaleUploadJournalValidation

    def read_window(self, offset: int, maximum_octets: int) -> bytes:
        self._require_current()
        return self.source.read_window(offset, maximum_octets)

    def read_exact(self, offset: int, octets: int) -> bytes:
        self._require_current()
        return self.source.read_exact(offset, octets)


@dataclass(slots=True)
class _Slot:
    lock: Lock = field(default_factory=Lock)
    session: JournalValidationSession | None = None
    resources: ExitStack = field(default_factory=ExitStack)
    closing: bool = False
    job: jobs.ValidationJob | None = None
    source: _OwnedSource | None = None
    work: source_owner._UploadJournalValidationWork | None = None
    catalog_sha256: str | None = None

    def retain_session(self, session: JournalValidationSession) -> None:
        if self.session is not None:
            raise RuntimeError("validation slot already retains Native storage")
        self.session = session


@dataclass(frozen=True, slots=True)
class ValidationAdvance:
    progressed: bool
    outcome: source_owner._UploadJournalValidationOutcome | None = None


def _pool_declaration(mode: str) -> Declaration:
    reserve = mode == "_reserve"

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def zero_octets() -> Expression:
        return Expression("constant", value=0, unit=OCTETS)

    return Declaration(
        id="riverhog-server.validation-pool-"
        + mode.removeprefix("_").replace("_", "-")
        + ".growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner."
            + mode,
        ),
        scope="existing owner pool reservation or exact active-key snapshot",
        starts_at="accept the configured pool and selected immutable source key",
        ends_at="return a nonblocking reservation/key snapshot or reject a closed owner",
        dimensions=(
            Dimension(
                "configured_slots", COUNT, "Positive operator-configured live Session capacity."
            ),
            Dimension(
                "pending_slots",
                COUNT,
                "Existing reservations at entry, within configured capacity.",
            ),
            Dimension(
                "input_key_octets",
                OCTETS,
                "Logical scalar widths of the selected reservation key; zero for a snapshot.",
            ),
            Dimension(
                "retained_key_octets",
                OCTETS,
                "Sum of scalar widths across all currently reserved keys.",
            ),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "equal",
                    count(1),
                    "Exactly one pool mutex acquisition/release, including rejection.",
                )
                for metric in ("pool_lock_acquisitions", "pool_lock_releases")
            ),
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper",
                    count(int(reserve)),
                    (
                        "A reservation uses each fixed-shape operation at most once; "
                        "snapshots use none."
                    ),
                )
                for metric in (
                    "map_lookups",
                    "map_length_reads",
                    "map_insertions",
                    "allocated_slots",
                    "slot_lock_attempts",
                    "returned_slots",
                )
            ),
            Bound(
                "map_key_visits",
                COUNT,
                "upper",
                count(0) if reserve else dimension("pending_slots"),
                "Reservations never enumerate the pool; snapshots visit each retained key once.",
            ),
            Bound(
                "returned_keys",
                COUNT,
                "upper",
                count(0) if reserve else dimension("pending_slots"),
                "A snapshot returns one collection/journal pair per current reservation.",
            ),
            Bound(
                "map_key_argument_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(Expression("constant", value=2), dimension("input_key_octets")),
                )
                if reserve
                else zero_octets(),
                "At most one lookup and one insertion carry the selected key.",
            ),
            Bound(
                "returned_key_octets",
                OCTETS,
                "upper",
                zero_octets() if reserve else dimension("retained_key_octets"),
                "Snapshot pairs exclude only the retained attempt component.",
            ),
            *(
                Bound(
                    metric,
                    COUNT,
                    "equal",
                    count(0),
                    "Pool-only operations perform no waiting or catalog/native/filesystem work.",
                )
                for metric in (
                    "blocking_slot_lock_attempts",
                    "catalog_calls",
                    "filesystem_calls",
                    "native_session_calls",
                )
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                zero_octets(),
                "Reservation and key snapshots never fetch source payload.",
            ),
        ),
        conditions=(
            (
                "The owner was constructed with positive configured concurrency; "
                "pool mutation uses its actual mutex."
            ),
            (
                "Dictionary keys are exact collection/journal/attempt scalar "
                "identities, not archive payload."
            ),
        ),
        invariants=(
            (
                "Busy slots and full pools return immediately without waiting or "
                "claiming durable work."
            ),
            (
                "Independent keys may be reserved while another slot is held, up "
                "to configured capacity."
            ),
            (
                "No catalog, filesystem, source validation or native Session work "
                "occurs under the pool mutex."
            ),
            "A reservation does not establish current source, worker or archive authority.",
        ),
        limitations=(
            (
                "Logical map, key and lock counts exclude dictionary internals, "
                "physical allocation and CPU/lock latency."
            ),
            (
                "Worker/source checks, provider construction, native execution, "
                "publication and closure retain their concrete owner costs."
            ),
        ),
    )


def _release_delegates() -> tuple[tuple[str, Callable[..., Any]], ...]:
    return (("release", JournalValidationSession.close), ("retain", JournalValidationSession.close))


def _release_declaration(mode: str) -> Declaration:
    native = growth_binding(JournalValidationSession.close).declaration
    lease = growth_binding(lease_journal_validation_workspace).declaration
    lease_closes = next(bound.expression for bound in lease.bounds if bound.metric == "close_calls")
    dropping = mode == "_drop"

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def delegated(tag: str, bound: Bound) -> Expression:
        return Expression(
            "delegate",
            name=tag,
            metric=bound.metric,
            terms=(dimension(tag + "_calls"),),
            arguments=tuple(
                (
                    item.name,
                    Expression(
                        "constant",
                        value=int(tag == "release" and item.name == "release_callbacks"),
                        unit=item.unit,
                    ),
                )
                for item in native.dimensions
            ),
        )

    callbacks = dimension("release_calls")
    if lease_closes.op != "constant" or lease_closes.unit != COUNT:
        raise ValueError("workspace release requires its fixed descriptor bound")
    descriptors = Expression("product", terms=(callbacks, replace(lease_closes, unit=())))
    limit = count(1) if dropping else dimension("pending_slots")
    return Declaration(
        id="riverhog-server.validation-owner-" + mode.removeprefix("_") + ".growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner."
            + mode,
        ),
        scope="concrete retained Session release with final workspace custody callbacks",
        starts_at="accept the selected owned slot or the existing configured pool",
        ends_at=(
            "close Native handles before removing confirmed reservations, preserving "
            "uncertain closure and borrowed-reader custody"
        ),
        dimensions=(
            Dimension("configured_slots", COUNT, "Positive configured pool capacity."),
            Dimension(
                "pending_slots", COUNT, "Reservations present at entry, within configured capacity."
            ),
            Dimension(
                "release_calls",
                COUNT,
                (
                    "Actual native close invocations that release final storage "
                    "and the owned lease callback."
                ),
            ),
            Dimension(
                "retain_calls",
                COUNT,
                (
                    "Actual other selected native close invocations, including "
                    "borrowed-reader and already-closed handles."
                ),
            ),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper",
                    count(1) if dropping else add(count(1), dimension("pending_slots")),
                    "Snapshot once and remove each confirmed reservation under the pool mutex.",
                )
                for metric in ("pool_lock_acquisitions", "pool_lock_releases")
            ),
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper",
                    limit,
                    "Only exact successfully closed reservations can be removed.",
                )
                for metric in ("pool_map_lookups", "pool_map_removals")
            ),
            Bound(
                "pool_map_clears",
                COUNT,
                "equal",
                count(0),
                "Unconfirmed reservations stay reachable; no pool-wide clear discards them.",
            ),
            Bound(
                "pool_slot_visits",
                COUNT,
                "equal",
                count(0) if dropping else dimension("pending_slots"),
                "Shutdown snapshots each current reservation once.",
            ),
            *(
                Bound(
                    metric,
                    COUNT,
                    "equal",
                    count(0) if dropping else dimension("pending_slots"),
                    (
                        "Shutdown waits on each slot outside the pool mutex; the drop "
                        "caller already owns its slot."
                    ),
                )
                for metric in ("slot_lock_attempts", "blocking_slot_lock_attempts")
            ),
            Bound(
                "native_close_calls",
                COUNT,
                "upper",
                limit,
                "Only selected nonempty native slots close.",
            ),
            Bound(
                "native_close_classes",
                COUNT,
                "equal",
                add(dimension("release_calls"), dimension("retain_calls")),
                (
                    "Actual release/retain calls partition the independently bounded "
                    "selected native invocations."
                ),
            ),
            Bound(
                "lease_release_callbacks",
                COUNT,
                "equal",
                callbacks,
                ("Only final storage release invokes the actual transferred lease callback."),
            ),
            Bound(
                "lease_descriptor_closes",
                COUNT,
                "upper",
                descriptors,
                (
                    "Callback release is a subset of the owning complete lease "
                    "lifecycle's descriptor bound."
                ),
            ),
            *(
                Bound(
                    metric,
                    COUNT,
                    "equal",
                    count(0),
                    (
                        "Retained shutdown performs no catalog query, source fetch, path "
                        "probe, directory walk, deletion or recreation."
                    ),
                )
                for metric in (
                    "catalog_calls",
                    "path_probes",
                    "path_opens",
                    "path_syncs",
                    "path_mutations",
                )
            ),
            *(
                Bound(
                    bound.metric,
                    bound.unit,
                    "upper",
                    add(delegated("release", bound), delegated("retain", bound), descriptors)
                    if bound.metric == "closed_files"
                    else add(delegated("release", bound), delegated("retain", bound)),
                    (
                        "Include the actual native close primitives and all explicit "
                        "final-lease descriptor release."
                    ),
                )
                for bound in native.bounds
            ),
            *(
                Bound(
                    tag + "_calls",
                    COUNT,
                    "equal",
                    dimension(tag + "_calls"),
                    "Observe the exact final-release or retained-reader invocation class.",
                )
                for tag in ("release", "retain")
            ),
            *(
                Bound(
                    tag + "_calls_limit",
                    COUNT,
                    "upper",
                    limit,
                    "Each close class stays within the selected live slots.",
                )
                for tag in ("release", "retain")
            ),
            *(
                Bound(
                    tag + "_" + bound.metric,
                    bound.unit,
                    bound.relation,
                    delegated(tag, bound),
                    ("Preserve every actual native close metric and its exact callback domain."),
                )
                for tag in ("release", "retain")
                for bound in native.bounds
            ),
        ),
        conditions=(
            "Slots and native Sessions were opened by this concrete owner over retained storage.",
            (
                "Final-reader release linearizes under storage ownership. Actual release and "
                "retention classes partition the invocations bounded by the entry pool snapshot."
            ),
            (
                "The drop caller holds its slot lock; shutdown snapshots under "
                "the pool mutex before waiting on each slot outside it."
            ),
            (
                "Storage callbacks are the exact transferred workspace lease "
                "release, whose descriptor cost is included here."
            ),
            (
                "Native close has zero unused opening/control dimensions; its "
                "callback dimension distinguishes final release from retained "
                "readers or prior closure."
            ),
        ),
        invariants=(
            "Database, filesystem and storage callback execution occurs outside the pool mutex.",
            "A mismatched replacement reservation remains intact when an older slot is dropped.",
            "Unconfirmed closure retains its closing reservation for a later retry.",
            "One failed slot does not prevent shutdown from closing independent slots.",
            (
                "Borrowed readers preserve native storage and kernel custody "
                "through their own final close."
            ),
            (
                "No retained data, source binding, job progress or accepted native"
                " checkpoint is deleted or recreated."
            ),
        ),
        limitations=(
            (
                "Logical lock, database, file and callback counts exclude lock "
                "waits, database-internal I/O and physical allocation/CPU latency."
            ),
            (
                "Deferred final-reader work belongs to that reader's release "
                "scope; it is observed as retained custody here."
            ),
            (
                "Native execution, source/worker checks, opening, publication and "
                "retirement retain their own concrete costs."
            ),
        ),
    )


def _sweep_children() -> tuple[tuple[str, Callable[..., Any]], ...]:
    return (("source", _OwnedSource.verify_source), ("drop", UploadJournalValidationOwner._drop))


def _sweep_declaration() -> Declaration:
    children = {tag: growth_binding(operation) for tag, operation in _sweep_children()}

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def add(terms: tuple[Expression, ...], unit: tuple[tuple[str, int], ...]) -> Expression:
        if not terms:
            return Expression("constant", value=0, unit=unit)
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    units: dict[str, tuple[tuple[str, int], ...]] = {}
    for binding in children.values():
        for bound in binding.declaration.bounds:
            if bound.metric in units and units[bound.metric] != bound.unit:
                raise ValueError("concrete child metrics must retain matching units")
            units[bound.metric] = bound.unit

    def child(tag: str, metric: str, unit: tuple[tuple[str, int], ...]) -> Expression:
        binding = children[tag]
        selected_metric = (
            "sql_statements" if tag == "source" and metric == "catalog_calls" else metric
        )
        if selected_metric not in {bound.metric for bound in binding.declaration.bounds}:
            return Expression("constant", value=0, unit=unit)
        arguments = {
            item.name: dimension(tag + "_" + item.name) for item in binding.declaration.dimensions
        }
        return Expression(
            "product",
            terms=(
                Expression("constant", value=1, unit=(("count", -1),)),
                dimension(tag + "_calls"),
                bound_envelope(binding.operation, selected_metric, arguments),
            ),
        )

    own = {
        "pool_lock_acquisitions": count(1),
        "pool_lock_releases": count(1),
        "pool_slot_visits": dimension("pending_slots"),
        "slot_lock_attempts": dimension("pending_slots"),
        "blocking_slot_lock_attempts": count(0),
    }
    return Declaration(
        id="riverhog-server.validation-owner-stale-sweep.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner.release_stale",
        ),
        scope="concrete nonblocking stale-slot sweep with heterogeneous source and closure calls",
        starts_at="snapshot existing reservations under the pool mutex",
        ends_at="finish the bounded pass or propagate the first non-stale provider failure",
        dimensions=(
            Dimension("configured_slots", COUNT, "Positive operator-configured live capacity."),
            Dimension(
                "pending_slots",
                COUNT,
                ("Actual reservations in the entry snapshot, within configured capacity."),
            ),
            *(
                Dimension(
                    tag + "_calls",
                    COUNT,
                    ("Actual child invocations, independently bounded by the entry snapshot."),
                )
                for tag in children
            ),
            *(
                Dimension(
                    tag + "_" + item.name,
                    item.unit,
                    (
                        "Upper envelope of this dimension over actual selected child "
                        "inputs; empty child domains may use zero. "
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
                        (
                            *tuple(child(tag, metric, unit) for tag in children),
                            *((own[metric],) if metric in own else ()),
                        ),
                        unit,
                    ),
                    (
                        "Actual own work and monotone source-owned child bounds over "
                        "dominating input envelopes."
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
                    (
                        "Every actual invocation retains its independently validated input"
                        " and primitive sample."
                    ),
                )
                for tag in children
            ),
            Bound(
                "source_calls_limit",
                COUNT,
                "upper",
                dimension("pending_slots"),
                "Each selected nonbusy source is checked at most once.",
            ),
            Bound(
                "drop_calls_limit",
                COUNT,
                "upper",
                dimension("pending_slots"),
                "Each nonbusy stale or already-closing reservation can be dropped at most once.",
            ),
            *(
                Bound(
                    tag + "_" + bound.metric,
                    bound.unit,
                    "upper",
                    child(tag, bound.metric, bound.unit),
                    (
                        "Preserve actual aggregate child observations without asserting "
                        "identical per-call dimensions."
                    ),
                )
                for tag, binding in children.items()
                for bound in binding.declaration.bounds
            ),
        ),
        conditions=(
            (
                "Every actual source/drop dimension is dominated by its declared "
                "per-call envelope; individual child declarations and samples "
                "remain authoritative."
            ),
            (
                "All numeric source expressions use monotone nonnegative "
                "arithmetic. Inlining substitutes only owned expressions and typed"
                " invocation multiplicity; it introduces no new expression "
                "operation."
            ),
            (
                "Source checks and drops are independently bounded by the entry "
                "reservation count; the configured capacity is a live resource "
                "admission."
            ),
        ),
        invariants=(
            (
                "Only reservation snapshots occur under the pool mutex; source "
                "checks, native closure and lease callbacks execute outside it."
            ),
            (
                "Each slot lock is attempted without waiting; a busy or "
                "not-yet-opened slot performs no source query."
            ),
            (
                "Actual stale custody closes only that selected slot, preserving "
                "accepted native state and independent reader leases."
            ),
            (
                "Unexpected provider failures propagate without fabricated "
                "validation progress or acceptance."
            ),
        ),
        limitations=(
            (
                "Logical map, lock, SQL, byte and kernel observations exclude "
                "internal B-tree work, physical allocation, I/O and CPU/lock "
                "latency."
            ),
            (
                "Opening, active semantic execution, publication and retirement "
                "retain their complete concrete costs."
            ),
        ),
    )


def _open_children() -> tuple[tuple[str, Callable[..., Any]], ...]:
    return (
        ("construct_source", UploadJournalValidationSource.__init__),
        ("source", UploadJournalValidationSource.verify_current),
        ("construct_certificate", UploadJournalGraphCertificates.__init__),
        ("select", jobs.select_job),
        ("outcome", jobs.outcome),
        ("creation", jobs.creation_current),
        ("claim", jobs.claim_worker),
        ("native", cast(Any, JournalValidationSession.open_retained).__func__),
        ("current", _OwnedSource.verify_current),
        ("progress", JournalValidationSession.progress),
        ("publish", jobs.publish_progress),
    )


def _open_declaration() -> Declaration:
    children = {tag: growth_binding(operation) for tag, operation in _open_children()}

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def add(terms: tuple[Expression, ...], unit: tuple[tuple[str, int], ...]) -> Expression:
        if not terms:
            return Expression("constant", value=0, unit=unit)
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def child(tag: str, metric: str, unit: tuple[tuple[str, int], ...]) -> Expression:
        binding = children[tag]
        metrics = {bound.metric for bound in binding.declaration.bounds}
        aliases = {
            "json_decoded_octets": "decoded_json_octets",
            "json_encoded_octets": "encoded_json_octets",
            "hashed_octets": "native_hashed_octets",
        }
        selected = metric if metric in metrics else aliases.get(metric, metric)
        if metric == "driver_statements" and selected not in metrics:
            selected = "sql_statements"
        if selected not in metrics:
            return Expression("constant", value=0, unit=unit)
        value = bound_envelope(
            binding.operation,
            selected,
            {
                item.name: dimension(tag + "_" + item.name)
                for item in binding.declaration.dimensions
            },
        )
        if tag == "source" and metric == "driver_statements":
            value = scale(8, value)
        return Expression(
            "product",
            terms=(
                Expression("constant", value=1, unit=(("count", -1),)),
                dimension(tag + "_calls"),
                value,
            ),
        )

    units = {
        "sql_statements": COUNT,
        "driver_statements": COUNT,
        "returned_rows": COUNT,
        "returned_metadata_octets": OCTETS,
        "bound_parameter_octets": OCTETS,
        "changed_rows": COUNT,
        "sqlite_vm_instructions": COUNT,
        "source_payload_octets": OCTETS,
        "json_decoded_octets": OCTETS,
        "json_encoded_octets": OCTETS,
        "hashed_octets": OCTETS,
        "uuid_values": COUNT,
        "opened_databases": COUNT,
        "closed_databases": COUNT,
        "opened_files": COUNT,
        "closed_files": COUNT,
        "file_syncs": COUNT,
        "directory_scans": COUNT,
        "directory_entries": COUNT,
        "unlinks": COUNT,
        "removed_directories": COUNT,
    }
    extra = {
        "returned_rows": scale(7, dimension("source_calls")),
        "returned_metadata_octets": add(
            (
                child("source", "binding_octets", OCTETS),
                Expression(
                    "product",
                    terms=(scale(8, dimension("source_calls")), dimension("metadata_row_octets")),
                ),
            ),
            OCTETS,
        ),
        "sqlite_vm_instructions": scale(8192, dimension("source_calls")),
        "json_encoded_octets": scale(2, dimension("binding_output_octets")),
    }
    limits = {tag: 2 if tag == "current" else 1 for tag in children}
    lease = growth_binding(lease_journal_validation_workspace)
    return Declaration(
        id="riverhog-server.validation-owner-open.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner._open",
        ),
        scope="concrete retained opening, exact provider checks and native-to-catalog handoff",
        starts_at="accept the selected finite source, reserved slot and fixed contract catalog",
        ends_at=(
            "return a terminal selection, transfer storage custody or propagate explicit rejection"
        ),
        dimensions=(
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                "Largest native control-row width over selected catalog inputs.",
            ),
            Dimension(
                "binding_output_octets",
                OCTETS,
                "Combined complete source and certificate binding output widths.",
            ),
            *(
                Dimension("lease_" + item.name, item.unit, item.meaning)
                for item in lease.declaration.dimensions
            ),
            *(
                Dimension(
                    tag + "_calls",
                    COUNT,
                    (
                        "Actual invocations, independently bounded by the fixed opening "
                        "control flow."
                    ),
                )
                for tag in children
            ),
            *(
                Dimension(
                    tag + "_" + item.name,
                    item.unit,
                    "Dominating per-call input envelope. " + item.meaning,
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
                        (
                            *tuple(child(tag, metric, unit) for tag in children),
                            *((extra[metric],) if metric in extra else ()),
                        ),
                        unit,
                    ),
                    (
                        "Own primitives and complete owned numeric child bounds; native "
                        "and catalog aliases retain the same primitive units."
                    ),
                )
                for metric, unit in units.items()
            ),
            *(
                Bound(
                    "lease_" + bound.metric,
                    bound.unit,
                    "upper",
                    bound_envelope(
                        lease_journal_validation_workspace,
                        bound.metric,
                        {
                            item.name: dimension("lease_" + item.name)
                            for item in lease.declaration.dimensions
                        },
                    ),
                    (
                        "This call's actual acquisition and immediate rejection-release "
                        "work is bounded by the complete owned lease lifetime; transferred"
                        " final release is observed by its later owner."
                    ),
                )
                for bound in lease.declaration.bounds
            ),
            Bound(
                "own_path_probes",
                COUNT,
                "upper",
                count(1),
                (
                    "Only the exact selected snapshot existence is probed outside the "
                    "lease and native providers."
                ),
            ),
            *(
                Bound(
                    tag + "_calls",
                    COUNT,
                    "equal",
                    dimension(tag + "_calls"),
                    "Preserve every actual independently validated child invocation.",
                )
                for tag in children
            ),
            *(
                Bound(
                    tag + "_calls_limit",
                    COUNT,
                    "upper",
                    count(limit),
                    (
                        "The opening control flow independently limits provider "
                        "invocations; native opening and progress each make at most one "
                        "concrete currentness check."
                    ),
                )
                for tag, limit in limits.items()
            ),
            *(
                Bound(
                    tag + "_" + bound.metric,
                    bound.unit,
                    "upper",
                    child(tag, bound.metric, bound.unit),
                    (
                        "Actual aggregate child primitives retain their original numeric "
                        "bounds and independent inputs."
                    ),
                )
                for tag, binding in children.items()
                for bound in binding.declaration.bounds
            ),
        ),
        conditions=(
            (
                "Each actual child dimension is dominated by its supplied monotone"
                " input envelope; children retain independently validated samples."
            ),
            (
                "The catalog is supplied explicitly. Metadata envelopes include "
                "exact source, certificate, old/new job and native checkpoint "
                "controls, not payload bytes."
            ),
            (
                "Lease observations cover only work actually performed during this"
                " opening. A transferred context is not represented as a completed"
                " lease invocation."
            ),
        ),
        invariants=(
            (
                "All provider, native, filesystem and storage callbacks execute "
                "outside the pool mutex."
            ),
            (
                "Only a current opening job may create storage; active accepted "
                "work never recreates a missing database."
            ),
            (
                "Native construction transfers the exact kernel lease to shared "
                "storage before catalog progress publication; borrowed readers "
                "retain that custody."
            ),
            (
                "Busy custody or stale selection performs no semantic replay and "
                "does not fabricate accepted progress."
            ),
            (
                "Completed and failed jobs return their exact detached outcome "
                "without opening native storage."
            ),
        ),
        limitations=(
            (
                "Logical primitives exclude internal index comparisons, physical "
                "allocation, I/O, and CPU or lock latency."
            ),
            (
                "Active semantic advancement and later storage release retain "
                "separately observed concrete costs."
            ),
        ),
    )


def _advance_children() -> tuple[tuple[str, Callable[..., Any]], ...]:
    return (
        ("sweep", UploadJournalValidationOwner.release_stale),
        ("reserve", UploadJournalValidationOwner._reserve),
        ("open", UploadJournalValidationOwner._open),
        ("progress", JournalValidationSession.progress),
        ("native", JournalValidationSession.advance),
        ("result", JournalValidationSession.result),
        ("publish", jobs.publish_progress),
        ("drop", UploadJournalValidationOwner._drop),
        ("current", _OwnedSource.verify_current),
        ("window", _OwnedSource.read_window),
        ("exact", _OwnedSource.read_exact),
        ("certificate", UploadJournalGraphCertificates.edge),
    )


def _advance_declaration() -> Declaration:
    children = {tag: growth_binding(operation) for tag, operation in _advance_children()}

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def add(terms: tuple[Expression, ...], unit: tuple[tuple[str, int], ...]) -> Expression:
        if not terms:
            return Expression("constant", value=0, unit=unit)
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def times(calls: Expression, value: Expression) -> Expression:
        return Expression(
            "product", terms=(Expression("constant", value=1, unit=(("count", -1),)), calls, value)
        )

    def child(tag: str, metric: str, unit: tuple[tuple[str, int], ...]) -> Expression:
        binding = children[tag]
        metrics = {bound.metric for bound in binding.declaration.bounds}
        aliases = {
            "json_decoded_octets": "decoded_json_octets",
            "json_encoded_octets": "encoded_json_octets",
            "hashed_octets": "native_hashed_octets",
            "pool_map_lookups": "map_lookups",
            "pool_map_insertions": "map_insertions",
            "pool_map_removals": "map_removals",
        }
        selected = metric if metric in metrics else aliases.get(metric, metric)
        if metric == "driver_statements" and selected not in metrics:
            selected = "sql_statements"
        if selected not in metrics:
            return Expression("constant", value=0, unit=unit)
        arguments = {
            item.name: dimension(tag + "_" + item.name) for item in binding.declaration.dimensions
        }
        envelope = bound_envelope(binding.operation, selected, arguments)
        if metric == "closed_files" and tag == "drop":
            envelope = add(
                (
                    envelope,
                    bound_envelope(binding.operation, "lease_descriptor_closes", arguments),
                ),
                unit,
            )
        if metric in {"opened_files", "closed_files"} and tag == "open":
            envelope = add(
                (
                    envelope,
                    bound_envelope(
                        binding.operation,
                        "lease_" + ("open_calls" if metric == "opened_files" else "close_calls"),
                        arguments,
                    ),
                ),
                unit,
            )
        return times(dimension(tag + "_calls"), envelope)

    units = {
        "sql_statements": COUNT,
        "driver_statements": COUNT,
        "returned_rows": COUNT,
        "returned_metadata_octets": OCTETS,
        "bound_parameter_octets": OCTETS,
        "changed_rows": COUNT,
        "sqlite_vm_instructions": COUNT,
        "source_payload_octets": OCTETS,
        "loaded_payload_octets": OCTETS,
        "assembled_piece_octets": OCTETS,
        "returned_octets": OCTETS,
        "json_decoded_octets": OCTETS,
        "json_encoded_octets": OCTETS,
        "hashed_octets": OCTETS,
        "uuid_values": COUNT,
        "native_schema_calls": COUNT,
        "historical_payload_octets": OCTETS,
        "source_port_octets": OCTETS,
        "opened_databases": COUNT,
        "closed_databases": COUNT,
        "opened_files": COUNT,
        "closed_files": COUNT,
        "file_syncs": COUNT,
        "directory_scans": COUNT,
        "directory_entries": COUNT,
        "unlinks": COUNT,
        "removed_directories": COUNT,
        "pool_lock_acquisitions": COUNT,
        "pool_lock_releases": COUNT,
        "pool_map_lookups": COUNT,
        "pool_map_insertions": COUNT,
        "pool_map_removals": COUNT,
        "pool_slot_visits": COUNT,
        "slot_lock_attempts": COUNT,
        "blocking_slot_lock_attempts": COUNT,
    }
    tail_width = Expression(
        "product", terms=(dimension("result_calls"), dimension("metadata_row_octets"))
    )
    tail_vm = times(
        dimension("result_calls"),
        add(
            (
                count(128),
                Expression(
                    "product",
                    terms=(
                        count(256),
                        Expression(
                            "log2_ceil",
                            terms=(add((count(2), dimension("native_retained_rows")), COUNT),),
                        ),
                    ),
                ),
            ),
            COUNT,
        ),
    )
    extra = {
        "sql_statements": dimension("result_calls"),
        "driver_statements": dimension("result_calls"),
        "returned_rows": dimension("result_calls"),
        "returned_metadata_octets": tail_width,
        "bound_parameter_octets": tail_width,
        "json_decoded_octets": scale(2, tail_width),
        "sqlite_vm_instructions": tail_vm,
    }
    limits = {
        tag: count(value)
        for tag, value in {
            "sweep": 1,
            "reserve": 1,
            "open": 1,
            "progress": 2,
            "native": 1,
            "result": 1,
            "publish": 2,
            "drop": 2,
            "current": 4,
            "window": 1,
            "exact": 1,
        }.items()
    }
    limits["certificate"] = dimension("row_budget")
    return Declaration(
        id="riverhog-server.validation-owner-advance.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.services.upload_journal_validation_owner.UploadJournalValidationOwner.advance",
        ),
        scope=(
            "concrete bounded retained execution, provider callbacks and exact catalog publication"
        ),
        starts_at=(
            "accept the selected finite source, sealed catalog and explicit native work budget"
        ),
        ends_at="return checked progress/outcome or propagate an explicit rejected control call",
        dimensions=(
            Dimension("configured_slots", COUNT, "Positive operator-selected live capacity."),
            Dimension(
                "pending_slots",
                COUNT,
                ("Actual reservations at entry, within configured capacity."),
            ),
            Dimension(
                "row_budget",
                COUNT,
                ("Positive native row/semantic-step budget supplied for this call."),
            ),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                (
                    "Complete native "
                    "checkpoint/result-reference"
                    " and catalog control width,"
                    " excluding payload."
                ),
            ),
            Dimension(
                "error_control_octets",
                OCTETS,
                (
                    "Complete single-string "
                    "native/storage error argument "
                    "received by this owner before its "
                    "bounded detached message slice."
                ),
            ),
            *(
                Dimension(
                    tag + "_calls",
                    COUNT,
                    (
                        "Actual invocations, independently capped by"
                        " the owner and native control flow."
                    ),
                )
                for tag in children
            ),
            *(
                Dimension(
                    tag + "_" + item.name,
                    item.unit,
                    (
                        "Dominating per-call input "
                        "envelope; actual child inputs "
                        "remain independently validated. "
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
                        (
                            *tuple(child(tag, metric, unit) for tag in children),
                            *((extra[metric],) if metric in extra else ()),
                        ),
                        unit,
                    ),
                    (
                        "Complete child "
                        "primitives plus the "
                        "parent's exact "
                        "returned "
                        "tail-reference "
                        "handling; aliases "
                        "retain identical "
                        "units and ownership."
                    ),
                )
                for metric, unit in units.items()
            ),
            Bound(
                "slot_lock_releases",
                COUNT,
                "upper",
                add((dimension("pending_slots"), count(1)), COUNT),
                (
                    "At most one release "
                    "per nonbusy "
                    "entry-sweep slot and"
                    " one release of the "
                    "selected "
                    "reservation."
                ),
            ),
            Bound(
                "error_formatted_octets",
                OCTETS,
                "upper",
                dimension("error_control_octets"),
                (
                    "The complete "
                    "received error "
                    "string is formatted "
                    "once before the "
                    "detached message is "
                    "sliced; truncation "
                    "does not hide this "
                    "work."
                ),
            ),
            *(
                Bound(
                    tag + "_calls",
                    COUNT,
                    "equal",
                    dimension(tag + "_calls"),
                    ("Every actual admitted invocation retains its real child scope and inputs."),
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
                        "One owner control path "
                        "bounds these invocations "
                        "independently of the "
                        "child records."
                    ),
                )
                for tag, limit in limits.items()
            ),
            Bound(
                "source_read_calls_limit",
                COUNT,
                "upper",
                count(1),
                (
                    "A native unit either"
                    " scans a source "
                    "window or loads one "
                    "pending exact atom; "
                    "it does not do both."
                ),
            ),
            *(
                Bound(
                    tag + "_" + bound.metric,
                    bound.unit,
                    "upper",
                    child(tag, bound.metric, bound.unit),
                    (
                        "Preserve every "
                        "actual aggregate "
                        "child primitive with"
                        " its source-owned "
                        "numeric bound."
                    ),
                )
                for tag, binding in children.items()
                for bound in binding.declaration.bounds
            ),
        ),
        conditions=(
            (
                "The explicit native budget and selected source/catalog/worker "
                "inputs satisfy their provider-owned admission contracts. Every "
                "child input is dominated by its declared envelope."
            ),
            (
                "Only derivation/specialization stage two requests a graph "
                "certificate. Each semantic program step requests at most one, "
                "within the native row/step budget."
            ),
            (
                "The native row envelope includes the completed selected frame "
                "whenever a result is borrowed; its tail is an indexed reference "
                "read without historical payload."
            ),
            (
                "Caught native/storage errors carry their owning complete "
                "single-string control argument; detached-message truncation is "
                "not a work bound."
            ),
        ),
        invariants=(
            (
                "Catalog, filesystem, provider and native work executes outside "
                "the pool mutex; slot admission is nonblocking."
            ),
            (
                "Only one native unit executes. Capacity retains the same "
                "unfinished atom, and stale worker/source custody cannot publish "
                "accepted progress."
            ),
            (
                "Native acceptance precedes catalog publication. A replacement "
                "adopts exactly that accepted prefix without replaying or "
                "recreating lost state."
            ),
            (
                "Successful outcome fields belong to the exact finite source and "
                "actual retained result; borrowed storage keeps its independent "
                "custody."
            ),
        ),
        limitations=(
            (
                "Effective recorder/context rejection retains its actual failed "
                "native record and publishes only the last accepted checkpoint "
                "with a detached failure. Other failed checkpoint, "
                "semantic/capacity and interruption units retain separate "
                "evidence; rejected work is never presented as a successful "
                "transition."
            ),
            (
                "Logical primitives exclude SQLite comparison bytes, physical "
                "I/O/allocation and CPU, lock or fsync latency."
            ),
        ),
    )


class UploadJournalValidationOwner:
    """Keep unfinished native atoms warm across ordinary bounded scheduler units.

    Pool locks protect only reservations. Catalog, filesystem, native validation
    and Session closure run outside the pool mutex. A busy job or full pool
    returns immediately, allowing the application scheduler to select other work.
    """

    def __init__(self, factory: SessionFactory, root: Path, *, concurrency: int = 2) -> None:
        if type(concurrency) is not int or concurrency < 1:
            raise ValueError("journal validation concurrency must be positive")
        self.factory, self.root, self.concurrency = factory, root, concurrency
        self._lock = Lock()
        self._slots: dict[tuple[int, str, str], _Slot] = {}
        self._closed = False

    def _reserve(self, key: tuple[int, str, str]) -> _Slot | None:
        with self._lock:
            if self._closed:
                raise RuntimeError("journal validation owner is closed")
            slot = self._slots.get(key)
            if slot is None:
                if len(self._slots) >= self.concurrency:
                    return None
                slot = _Slot()
                self._slots[key] = slot
            # Nonblocking acquisition does no work while holding the pool mutex.
            return slot if slot.lock.acquire(blocking=False) else None

    def _drop(self, key: tuple[int, str, str], slot: _Slot) -> None:
        slot.closing = True
        if slot.session is not None:
            slot.session.close()
            slot.session = None
        slot.resources.close()
        with self._lock:
            if self._slots.get(key) is slot:
                del self._slots[key]

    def active_keys(self) -> tuple[tuple[int, str], ...]:
        with self._lock:
            return tuple((collection, journal) for collection, journal, _ in self._slots)

    def release_stale(self) -> None:
        with self._lock:
            slots = tuple(self._slots.items())
        errors = []
        for key, slot in slots:
            if not slot.lock.acquire(blocking=False):
                continue
            try:
                if slot.closing:
                    self._drop(key, slot)
                elif slot.source is not None:
                    try:
                        slot.source.verify_source()
                    except source_owner._StaleUploadJournalValidation:
                        self._drop(key, slot)
            except BaseException as error:
                errors.append(error)
            finally:
                slot.lock.release()
        if errors:
            raise errors[0]

    def close(self) -> None:
        with self._lock:
            self._closed = True
            slots = tuple(self._slots.items())
        errors = []
        for key, slot in slots:
            try:
                with slot.lock:
                    self._drop(key, slot)
            except BaseException as error:
                errors.append(error)
        if errors:
            raise errors[0]

    def _open(
        self,
        source: source_owner._UploadJournalValidationWork,
        slot: _Slot,
        catalog: ContractCatalog,
    ) -> ValidationAdvance | None:
        selected = UploadJournalValidationSource(self.factory, source, catalog=catalog)
        if not selected.verify_current():
            return ValidationAdvance(False)
        certificates = UploadJournalGraphCertificates(self.factory, source, catalog=catalog)
        job = jobs.select_job(
            self.factory,
            source,
            source_binding=selected.binding.to_json(),
            certificate_binding=certificates.binding.to_json(),
        )
        if job is None:
            return ValidationAdvance(False)
        if job.state in {"complete", "failed"}:
            return ValidationAdvance(False, jobs.outcome(job))
        if job.state == "lost":
            raise LostJournalValidationWorkspace(job.failure or "retained validation was lost")
        if job.state not in {"opening", "active"}:
            return ValidationAdvance(False)
        slot.job = job
        creation_job = job
        resources = slot.resources
        try:
            directory = resources.enter_context(
                lease_journal_validation_workspace(
                    self.root,
                    job.workspace_id,
                    create=job.state == "opening",
                    creation_current=lambda: jobs.creation_current(
                        self.factory, source, creation_job
                    ),
                )
            )
            if directory is None:
                return ValidationAdvance(False)
            claimed = jobs.claim_worker(self.factory, source, job)
            if claimed is None:
                return ValidationAdvance(False)
            job = slot.job = claimed
            # Only unconfirmed initial creation may create a database. Active
            # accepted work never turns a missing file into a fresh validator.
            create = job.state == "opening" and not (directory / "snapshot.sqlite3").exists()
            owned_source = _OwnedSource(self.factory, selected, job, certificates)
            current = JournalValidationSession.open_retained(
                directory,
                owned_source,
                attempt_id=job.workspace_id,
                worker_epoch=job.worker_epoch,
                create=create,
                catalog=catalog,
                certificates=certificates,
                on_storage_close=resources.close,
                on_storage_open=slot.retain_session,
            )
            assert slot.session is current
            # Successful construction transfers the lease to final Native storage
            # release, including independently borrowed frozen readers.
            slot.resources = ExitStack()
            slot.source, slot.work = owned_source, source
            slot.catalog_sha256 = catalog.checkpoint_sha256()
            adopted = jobs.publish_progress(self.factory, source, job, current.progress())
            if adopted is None:
                raise source_owner._StaleUploadJournalValidation
            slot.job = adopted
            return None
        finally:
            # An early Native callback retains even partially initialized storage.
            # Its uncertain closure must precede release of application custody.
            if slot.session is None:
                resources.close()

    def advance(
        self,
        source: source_owner._UploadJournalValidationWork,
        *,
        catalog: ContractCatalog,
        budget: JournalValidationBudget = _DEFAULT_BUDGET,
    ) -> ValidationAdvance:
        self.release_stale()
        key = source.collection_id, source.journal_id, source.attempt_id
        slot = self._reserve(key)
        if slot is None:
            return ValidationAdvance(False)
        try:
            if slot.closing:
                self._drop(key, slot)
                return ValidationAdvance(False)
            if slot.session is None:
                terminal = self._open(source, slot, catalog)
                if terminal is not None:
                    self._drop(key, slot)
                    return terminal
            current, job = slot.session, slot.job
            if current is None or job is None:
                raise RuntimeError("validation reservation has no native owner")
            if slot.work != source or slot.catalog_sha256 != catalog.checkpoint_sha256():
                raise source_owner._StaleUploadJournalValidation
            before = current.progress()
            outcome = None
            try:
                after = current.advance(budget)
            except (ProvenanceValidationError, ValueError) as error:
                after = current.progress()
                outcome = source_owner._UploadJournalValidationOutcome(str(error)[:1000])
            else:
                if after.complete:
                    with current.result() as summary:
                        if (
                            summary.journal_id != source.journal_id
                            or summary.journal_sha256 != source.source_sha256
                            or summary.journal_bytes != source.source_bytes
                        ):
                            raise RuntimeError("native result differs from its exact source")
                        tail = summary.anchor["through"]
                        outcome = source_owner._UploadJournalValidationOutcome(
                            None,
                            len(summary.frames),
                            tail["entry_id"],
                            int(tail["sequence"]),
                            tail["json_sha256"],
                        )
            published = jobs.publish_progress(self.factory, source, job, after, outcome=outcome)
            if published is None:
                raise source_owner._StaleUploadJournalValidation
            slot.job = published
            if outcome is not None:
                self._drop(key, slot)
            return ValidationAdvance(after != before or outcome is not None, outcome)
        except JournalValidationCapacity:
            # A successfully opened atom stays warm; failed initialization retains
            # its early storage handle only until application cleanup confirms.
            if slot.source is None and not slot.closing:
                self._drop(key, slot)
            return ValidationAdvance(False)
        except source_owner._StaleUploadJournalValidation:
            if not slot.closing:
                self._drop(key, slot)
            return ValidationAdvance(False)
        except (
            LostJournalValidationWorkspace,
            InvalidJournalWorkspace,
            InvalidJournalGraphCertificate,
            sqlite3.DatabaseError,
        ) as error:
            try:
                if slot.job is not None:
                    jobs.publish_progress(
                        self.factory, source, slot.job, None, lost=str(error)[:1000]
                    )
            finally:
                if not slot.closing:
                    self._drop(key, slot)
            raise LostJournalValidationWorkspace(
                "retained native validation is unavailable"
            ) from error
        except BaseException:
            if not slot.closing:
                self._drop(key, slot)
            raise
        finally:
            slot.lock.release()


declare_growth(
    _source_fence_declaration("verify_source"), delegates=_source_fence_children("verify_source")
)(_OwnedSource.verify_source)
declare_growth(
    _source_fence_declaration("verify_current"), delegates=_source_fence_children("verify_current")
)(_OwnedSource.verify_current)

for _owned_read in (_OwnedSource.read_window, _OwnedSource.read_exact):
    declare_growth(
        _source_read_declaration(_owned_read.__name__),
        delegates=_source_fence_children(_owned_read.__name__),
    )(_owned_read)
del _owned_read

for _pool_operation in (
    UploadJournalValidationOwner._reserve,
    UploadJournalValidationOwner.active_keys,
):
    declare_growth(_pool_declaration(_pool_operation.__name__))(_pool_operation)
del _pool_operation

for _release_operation in (UploadJournalValidationOwner._drop, UploadJournalValidationOwner.close):
    declare_growth(
        _release_declaration(_release_operation.__name__), delegates=_release_delegates()
    )(_release_operation)
del _release_operation

declare_growth(_sweep_declaration(), delegates=_sweep_children())(
    UploadJournalValidationOwner.release_stale
)


declare_growth(_open_declaration(), delegates=_open_children())(UploadJournalValidationOwner._open)


declare_growth(_advance_declaration(), delegates=_advance_children())(
    UploadJournalValidationOwner.advance
)


source_owner._declare_service_validation_unit_growth()
