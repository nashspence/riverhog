"""Read an owner's confirmed terminal projection without advancing or replaying it."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import closing
from pathlib import Path
from typing import Any

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance_contracts import ContractCatalog

from ._journal_prefix import JournalPrefix
from ._session_checkpoint import ValidationLedger
from ._session_input import RetainedInput
from .errors import IncompleteSourceError
from .graph_certificates import JournalGraphCertificateBinding
from .journal import JournalSummary
from .journal_session import (
    JournalValidationProgress,
    JournalValidationSource,
    _checkpoint_progress,
)
from .journal_store import InvalidJournalWorkspace, JournalStore


def _result_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def metadata(multiplier: int) -> Expression:
        return Expression(
            "product",
            terms=(
                Expression("constant", value=multiplier),
                Expression("dimension", name="header_octets"),
            ),
        )

    def depth(name: str) -> Expression:
        return Expression(
            "log2_ceil",
            terms=(
                Expression(
                    "sum",
                    terms=(
                        count(2),
                        Expression("dimension", name=name),
                    ),
                ),
            ),
        )

    return Declaration(
        id="riverhog-provenance.retained-result-admission.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.journal_result.open_retained_journal_result",),
        scope="native-sqlite-owner-confirmed-frozen-result-admission",
        starts_at="admit the current owner's exact complete checkpoint and retained workspace",
        ends_at="return one independently owned frozen result without traversing its views",
        dimensions=(
            Dimension("journal_octets", OCTETS, "Exact octets in the confirmed journal."),
            Dimension("frozen_frames", COUNT, "Accepted frames in the retained projection."),
            Dimension("indexed_assertions", COUNT, "Retained active and retired assertion rows."),
            Dimension(
                "header_octets",
                OCTETS,
                "Largest complete logical metadata row among the workspace/algorithm markers, "
                "active graph headers, prefix, parent and input checkpoints, "
                "and exact tail reference; "
                "text is UTF-8 and scalar integers use canonical decimal text.",
            ),
        ),
        bounds=(
            Bound(
                "returned_handles", COUNT, "equal", count(1), "One independently closeable result."
            ),
            Bound(
                "opened_databases",
                COUNT,
                "equal",
                count(1),
                "One existing SQLite mode=ro database.",
            ),
            Bound(
                "source_probes",
                COUNT,
                "equal",
                count(2),
                "Current source checks before and after admission.",
            ),
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(64),
                "Fixed marker, graph and checkpoint point reads and connection PRAGMAs.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(32),
                "Fixed headers and indexed predecessor metadata; no resource census.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                metadata(32),
                "At most 32 rows, each bounded by the largest retained header row.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                metadata(64),
                "Repeated matching source/prefix/checkpoint bindings and scalar point keys.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "equal",
                count(0),
                "Read-only admission never advances native state.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No source range is requested.",
            ),
            Bound(
                "historical_payload_reads",
                COUNT,
                "equal",
                count(0),
                "No frame, assertion or in-flight frame payload column is read.",
            ),
            Bound(
                "open_cursors",
                COUNT,
                "equal",
                count(0),
                "Every admission cursor closes before the result is returned.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(2048),
                        Expression(
                            "product",
                            terms=(
                                count(512),
                                Expression(
                                    "sum",
                                    terms=(
                                        depth("frozen_frames"),
                                        depth("indexed_assertions"),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Native instruction envelope for fixed primary-key lookups; "
                "no scans or temporary sorts.",
            ),
        ),
        conditions=(
            "The trusted owner supplies its complete matching private format, "
            "source and catalog pins, "
            "and excludes writers and reclamation for every returned reader.",
            "The sealed contract catalog exposes its constructed cached checkpoint identity.",
            "SQLite uses the current workspace primary keys and fixed schema.",
        ),
        invariants=(
            "No new worker, source read, accepted-frame replay or checkpoint write occurs.",
            "An unavailable or mismatched workspace is never recreated or accepted.",
            "Closing one result cannot invalidate a different live reader.",
        ),
        limitations=(
            "Catalog construction when not supplied, the two source-probe implementations "
            "and owner filesystem leases require caller composition.",
            "Workspace creation/validation, caller view traversal/materialization, "
            "result disposal and reclamation are separate operations.",
            "Metadata decoding and hashing process the observed header octets; "
            "SQLite VM counts exclude internal comparisons, page I/O and physical CPU time.",
            "This successful-admission profile does not waive identity/resource failures "
            "or qualify another storage backend.",
        ),
    )


@declare_growth(_result_declaration())
def open_retained_journal_result(
    directory: Path,
    source: JournalValidationSource,
    *,
    confirmed: JournalValidationProgress,
    catalog: ContractCatalog | None = None,
    certificates: JournalGraphCertificateBinding | None = None,
    expected_anchor: Mapping[str, Any] | None = None,
    require_exact_tail: bool = False,
    on_storage_close: Callable[[], None] | None = None,
) -> JournalSummary:
    """Return one independently owned, read-only, exactly confirmed frozen result.

    The trusted owner supplies current immutable source authority, its confirmed
    complete progress and private workspace custody. This does not admit imported
    databases or recreate missing work. No worker epoch is replaced, source range
    is read, accepted frame is replayed, or native checkpoint is written.

    The caller excludes writers and reclamation for every resulting reader. On
    successful return the storage owns on_storage_close through the last borrowed
    handle. A failed constructor leaves that resource callback with the caller.
    """
    selected = catalog or ContractCatalog()
    binding = source.binding
    if confirmed.complete is not True or confirmed.phase != "done":
        raise InvalidJournalWorkspace("a frozen result requires confirmed complete progress")
    if binding.catalog_sha256 != selected.checkpoint_sha256():
        raise InvalidJournalWorkspace("frozen result catalog differs from its current owner")
    if source.verify_current() is not True:
        raise IncompleteSourceError("frozen journal source is no longer current")
    prefix_binding = binding.to_json()
    if certificates is not None:
        if certificates.source != binding:
            raise InvalidJournalWorkspace("frozen graph evidence belongs to another source")
        prefix_binding = canonical_json_bytes(
            {"source": prefix_binding, "graph_certificate": certificates.to_json()}
        ).decode()
    store = JournalStore.open_retained(directory, read_only=True)
    try:
        prefix = JournalPrefix(
            store.db,
            attempt_id=confirmed.attempt_id,
            source_binding=prefix_binding,
            catalog=selected,
            require_profiles=binding.require_profiles,
            expected_anchor=expected_anchor,
            require_exact_tail=require_exact_tail,
        )
        ledger = ValidationLedger(
            store.db,
            attempt_id=confirmed.attempt_id,
            binding=prefix.binding,
            worker_epoch=confirmed.worker_epoch,
        )
        snapshot = ledger.load()
        source_cursor = RetainedInput(ledger, source)
        progress = _checkpoint_progress(
            confirmed.attempt_id, ledger, snapshot, source_cursor.cursor.byte_offset
        )
        if progress != confirmed or not source_cursor.complete or source_cursor.pending:
            raise InvalidJournalWorkspace("frozen result differs from its confirmed checkpoint")
        with closing(
            store.db.execute("SELECT 1 FROM journal_validation_frame WHERE singleton=1")
        ) as rows:
            if rows.fetchone() is not None:
                raise InvalidJournalWorkspace("frozen result has an unfinished frame")
        state = prefix.finish()
        if (
            state.journal_id != binding.journal_id
            or state.byte_count != int(binding.source_bytes)
            or state.frame_count != int(binding.frame_count)
            or state.hexdigest != binding.source_sha256
            or store.frame_count != state.frame_count
        ):
            raise InvalidJournalWorkspace("frozen result differs from its exact source")
        if source.binding != binding or source.verify_current() is not True:
            raise IncompleteSourceError("frozen journal source changed during result admission")
        store.freeze()
        result = JournalSummary(
            state.journal_id,
            store.frames,
            store.graph_validation,
            store.retired,
            state.hexdigest,
            state.byte_count,
            store.origins,
        )
        object.__setattr__(result, "_store", store)
    except BaseException:
        store.close()
        raise
    store._storage.on_close = on_storage_close
    return result
