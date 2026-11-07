"""Admit exact sealed native results within the caller's catalog transaction."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from contextlib import ExitStack
from dataclasses import dataclass
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
from riverhog_provenance import (
    JOURNAL_FRAMING_ALGORITHM,
    JournalSummary,
    JournalValidationCapacity,
    JournalValidationProgress,
    open_retained_journal_result,
)
from riverhog_provenance.graph_certificates import JournalGraphCertificateBinding
from riverhog_provenance.replay_binding import JournalReplayBinding
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    lease_journal_validation_workspace,
)

from . import upload_journal_validation_jobs as jobs
from .upload_journal_result_admission_growth import declare_growth_for_sealed_admission

_SOURCE_FIELDS = (
    "collection_id",
    "journal_id",
    "state",
    "bytes",
    "sha256",
    "accepted_bytes",
    "capture_attempt_id",
    "capture_directory_id",
    "capture_algorithm",
    "capture_source_bytes",
    "capture_source_sha256",
    "capture_version",
    "capture_phase",
    "capture_byte_offset",
    "capture_frame_start",
    "capture_frame_count",
    "capture_failure",
    "validation_byte_offset",
    "validation_sequence",
    "failure",
    "terminal_entry_id",
    "terminal_sequence",
    "terminal_json_sha256",
)


class _SealedSource:
    def __init__(
        self,
        session: Session,
        source: Mapping[str, Any],
        job: jobs.ValidationJob,
        binding: JournalReplayBinding,
    ) -> None:
        self.session, self._binding = session, binding
        self.conditions = tuple(getattr(Journal, key) == value for key, value in source.items()) + (
            *(
                getattr(Job, key) == getattr(job, key)
                for key in jobs.ValidationJob.__dataclass_fields__
            ),
        )

    @property
    def binding(self) -> JournalReplayBinding:
        return self._binding

    def verify_current(self) -> bool:
        if not self.session.in_transaction():
            return False
        query = (
            select(Job.workspace_id)
            .select_from(Journal)
            .join(
                Job,
                and_(
                    Job.collection_id == Journal.collection_id, Job.journal_id == Journal.journal_id
                ),
            )
            .where(*self.conditions)
        )
        with self.session.no_autoflush, self.session.execute(query) as rows:
            return rows.one_or_none() is not None

    def read_window(self, offset: int, maximum_octets: int) -> bytes:
        raise RuntimeError("a confirmed frozen result cannot read source payload")

    def read_exact(self, offset: int, octets: int) -> bytes:
        raise RuntimeError("a confirmed frozen result cannot replay source payload")


def _confirmed(
    source: Mapping[str, Any],
    job: jobs.ValidationJob,
) -> tuple[
    JournalReplayBinding, JournalGraphCertificateBinding, JournalValidationProgress, dict[str, Any]
]:
    if job.state != "complete" or job.failure is not None or job.progress_json is None:
        raise LostJournalValidationWorkspace("sealed journal has no confirmed native result")
    binding = JournalReplayBinding.from_json(job.source_binding_json)
    certificates = JournalGraphCertificateBinding.from_json(job.certificate_binding_json)
    progress = jobs.decode_progress(job.progress_json)
    outcome = jobs.outcome(job)
    if (
        job.attempt_id != binding.attempt_id
        or certificates.source != binding
        or binding.require_profiles
        or source["capture_attempt_id"] != binding.attempt_id
        or source["capture_directory_id"] != binding.directory_id
        or source["journal_id"] != binding.journal_id
        or source["bytes"] != int(binding.source_bytes)
        or source["accepted_bytes"] != int(binding.source_bytes)
        or source["sha256"] != binding.source_sha256
        or source["capture_source_bytes"] != int(binding.source_bytes)
        or source["capture_source_sha256"] != binding.source_sha256
        or source["capture_version"] != int(binding.source_version) + 1
        or source["capture_algorithm"] != JOURNAL_FRAMING_ALGORITHM
        or source["capture_phase"] not in {"complete", "deferred"}
        or source["capture_frame_count"] != int(binding.frame_count)
        or source["failure"] is not None
        or source["validation_byte_offset"] != int(binding.source_bytes)
        or source["validation_sequence"] != int(binding.frame_count)
        or progress.attempt_id != job.workspace_id
        or progress.worker_epoch != job.worker_epoch
        or not progress.complete
        or progress.phase != "done"
        or outcome.failure is not None
        or outcome.frame_count != int(binding.frame_count)
        or outcome.terminal_entry_id != source["terminal_entry_id"]
        or outcome.terminal_sequence != source["terminal_sequence"]
        or outcome.terminal_sequence != int(binding.frame_count) - 1
        or outcome.terminal_json_sha256 != source["terminal_json_sha256"]
    ):
        raise LostJournalValidationWorkspace(
            "sealed journal differs from its confirmed native result"
        )
    anchor = {
        "journal_id": binding.journal_id,
        "through": {
            "entry_id": source["terminal_entry_id"],
            "sequence": str(source["terminal_sequence"]),
            "json_sha256": source["terminal_json_sha256"],
        },
        "prefix_sha256": binding.source_sha256,
        "prefix_bytes": binding.source_bytes,
    }
    return binding, certificates, progress, anchor


@dataclass(frozen=True, slots=True)
class SealedJournalResultAdmission:
    """Detached exact catalog confirmation; no connection or Native handle."""

    _source: tuple[tuple[str, Any], ...]
    job: jobs.ValidationJob
    binding: JournalReplayBinding
    certificates: JournalGraphCertificateBinding
    confirmed: JournalValidationProgress

    @property
    def anchor(self) -> dict[str, Any]:
        source = dict(self._source)
        return {
            "journal_id": self.binding.journal_id,
            "through": {
                "entry_id": source["terminal_entry_id"],
                "sequence": str(source["terminal_sequence"]),
                "json_sha256": source["terminal_json_sha256"],
            },
            "prefix_sha256": self.binding.source_sha256,
            "prefix_bytes": self.binding.source_bytes,
        }

    def verify_current(self, session: Session) -> bool:
        """Point-check the exact immutable source/job in this short transaction."""
        return _SealedSource(session, dict(self._source), self.job, self.binding).verify_current()


def prepare_upload_journal_result(
    session: Session,
    collection_id: int,
    journal_id: str,
    *,
    catalog: ContractCatalog,
) -> SealedJournalResultAdmission:
    """Select and validate immutable sealed metadata without opening Native data.

    The caller owns this bounded catalog transaction. After it closes, the
    admission carries no row cursor, session, lease or file. Native opening
    requires independent workspace custody and current checks against this exact
    admission; its original binding is never relabeled to another workspace.
    """
    if not session.in_transaction():
        raise RuntimeError("sealed result admission requires the caller's catalog transaction")
    with session.no_autoflush:
        with session.execute(
            select(*(getattr(Journal, field) for field in _SOURCE_FIELDS))
            .where(Journal.collection_id == collection_id, Journal.journal_id == journal_id)
            .with_for_update()
        ) as rows:
            selected_source = rows.mappings().one_or_none()
        if selected_source is None or selected_source["state"] != "sealed":
            raise KeyError(journal_id)
        source = dict(selected_source)
        with session.execute(
            select(*(getattr(Job, field) for field in jobs.ValidationJob.__dataclass_fields__))
            .where(Job.collection_id == collection_id, Job.journal_id == journal_id)
            .with_for_update()
        ) as rows:
            selected = rows.mappings().one_or_none()
        if selected is None:
            raise LostJournalValidationWorkspace("sealed journal lost its retained native job")
        job = jobs.ValidationJob(**dict(selected))
    try:
        binding, certificates, confirmed, _anchor = _confirmed(source, job)
        if binding.catalog_sha256 != catalog.checkpoint_sha256():
            raise LostJournalValidationWorkspace(
                "sealed result admission has another current contract catalog"
            )
    except (RuntimeError, ValueError) as error:
        raise LostJournalValidationWorkspace("sealed native result is unavailable") from error
    return SealedJournalResultAdmission(
        tuple(source.items()), job, binding, certificates, confirmed
    )


def _result_declaration() -> Declaration:
    native = growth_binding(open_retained_journal_result).declaration

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def total(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def scale(multiplier: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=multiplier), term))

    def depth(name: str) -> Expression:
        return Expression("log2_ceil", terms=(total(count(2), dimension(name)),))

    additions = {
        "sql_statements": count(1),
        "returned_rows": count(1),
        "returned_metadata_octets": dimension("header_octets"),
        "bound_parameter_octets": dimension("header_octets"),
        "sqlite_vm_instructions": total(
            count(1024), Expression("product", terms=(count(512), depth("frozen_frames")))
        ),
    }
    native_bounds = []
    for bound in native.bounds:
        expression = Expression(
            "delegate",
            name="native",
            metric=bound.metric,
            terms=(count(1),),
            arguments=tuple((value.name, dimension(value.name)) for value in native.dimensions),
        )
        if bound.metric in additions:
            expression = total(expression, additions[bound.metric])
        native_bounds.append(
            Bound(
                "native_" + bound.metric,
                bound.unit,
                "upper" if bound.metric in additions else bound.relation,
                expression,
                "Actual native admission and the caller's one indexed terminal-anchor lookup.",
            )
        )
    return Declaration(
        id="riverhog.sealed-journal-result-admission.growth/v1",
        authority="riverhog",
        subjects=("riverhog_core.services.upload_journal_result.open_upload_journal_result",),
        scope="sqlite-catalog-posix-sealed-result-admission",
        starts_at="accept the caller's active catalog transaction and exact sealed journal",
        ends_at="return one native result retaining its shared workspace lease",
        dimensions=(
            *native.dimensions,
            Dimension("catalog_journals", COUNT, "All catalog journal rows."),
            Dimension("catalog_jobs", COUNT, "All catalog native validation jobs."),
            Dimension(
                "catalog_row_octets",
                OCTETS,
                "Largest selected complete source or job metadata row; UTF-8 text and "
                "canonical decimal scalar integers, excluding SQL schema labels.",
            ),
            Dimension(
                "workspace_path_octets", OCTETS, "UTF-8 octets in the absolute database path."
            ),
            Dimension(
                "workspace_path_components", COUNT, "Components in the absolute database path."
            ),
        ),
        bounds=(
            *native_bounds,
            Bound(
                "catalog_statements",
                COUNT,
                "equal",
                count(4),
                "Two row selections and two current-authority probes.",
            ),
            Bound(
                "catalog_returned_rows",
                COUNT,
                "equal",
                count(4),
                "One row per catalog point query.",
            ),
            Bound(
                "catalog_metadata_octets",
                OCTETS,
                "upper",
                scale(4, dimension("catalog_row_octets")),
                "Selected metadata and two workspace identities.",
            ),
            Bound(
                "catalog_parameter_octets",
                OCTETS,
                "upper",
                scale(8, dimension("catalog_row_octets")),
                "Exact source/job predicates and selected primary keys.",
            ),
            Bound(
                "catalog_changed_rows",
                COUNT,
                "equal",
                count(0),
                "No catalog write or unrelated autoflush.",
            ),
            Bound(
                "catalog_payload_reads",
                COUNT,
                "equal",
                count(0),
                "No original or captured source payload column.",
            ),
            Bound(
                "catalog_open_cursors",
                COUNT,
                "equal",
                count(0),
                "Every catalog result closes before return.",
            ),
            Bound(
                "catalog_vm_instructions",
                COUNT,
                "upper",
                total(
                    count(4096),
                    Expression(
                        "product",
                        terms=(
                            count(1024),
                            total(depth("catalog_journals"), depth("catalog_jobs")),
                        ),
                    ),
                ),
                "SQLite primary-key and unique-source index lookups, without scans or temporary "
                "sorts.",
            ),
            Bound(
                "lease_opens",
                COUNT,
                "equal",
                count(4),
                "Existing root, coordinator, workspace and shared lease descriptors.",
            ),
            Bound(
                "lease_fsyncs",
                COUNT,
                "equal",
                count(3),
                "Existing coordinator, lease and workspace durability calls.",
            ),
            Bound(
                "lease_lock_calls",
                COUNT,
                "equal",
                count(3),
                "Nonblocking coordinator acquisition, shared lease acquisition and coordinator "
                "release.",
            ),
            Bound(
                "filesystem_metadata_calls",
                COUNT,
                "upper",
                total(count(16), scale(2, dimension("workspace_path_components"))),
                "Descriptor custody checks and native path admission/resolution.",
            ),
            Bound(
                "filesystem_path_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=1, unit=(("count", -1),)),
                        total(count(32), scale(2, dimension("workspace_path_components"))),
                        dimension("workspace_path_octets"),
                    ),
                ),
                "Logical path arguments across open, stat, lstat and readlink calls.",
            ),
        ),
        conditions=(
            "The caller supplies a constructed immutable contract catalog and keeps the selected "
            "source/job row locks for work requiring current authority.",
            "SQLite uses the maintained catalog indexes and native workspace schema; POSIX flock, "
            "private owner-created custody and a native absolute path without symlinked ancestors "
            "apply.",
        ),
        invariants=(
            "Admission never rereads source payload, replays validation, replaces a worker epoch "
            "or recreates lost state.",
            "The shared lease remains owned until the final independent reader closes; writers "
            "and reclamation remain excluded.",
            "Busy or unavailable resources leave publication pending without changing confirmed "
            "outcomes.",
        ),
        limitations=(
            "Catalog transaction acquisition/waits, fixture construction, prior validation, view "
            "traversal and final disposal/reclamation are separate operations.",
            "This successful SQLite/POSIX profile does not qualify another catalog or filesystem "
            "backend, physical I/O latency or machine CPU time.",
            "Metadata parsing and canonical identity checks process the selected logical "
            "metadata; path resolution retains its explicit path-length and component dimensions.",
        ),
    )


@declare_growth(_result_declaration(), delegates=(("native", open_retained_journal_result),))
def open_upload_journal_result(
    session: Session,
    root: Path,
    collection_id: int,
    journal_id: str,
    *,
    catalog: ContractCatalog,
) -> JournalSummary:
    """Own one read-only result without a new catalog writer or native worker.

    The caller owns the current catalog transaction and keeps its source/job row
    locks for work requiring current authority. Returned independent readers keep
    the exact frozen projection and shared kernel lease until their last close.
    Missing retained state is explicit; this never recreates or revalidates it.
    """
    admission = prepare_upload_journal_result(session, collection_id, journal_id, catalog=catalog)
    job = admission.job
    resources = ExitStack()
    transferred = False
    try:
        owned_source = _SealedSource(session, dict(admission._source), job, admission.binding)
        directory = resources.enter_context(
            lease_journal_validation_workspace(root, job.workspace_id, shared=True)
        )
        if directory is None:
            raise JournalValidationCapacity(
                "sealed native result is busy; publication remains pending"
            )
        result = open_retained_journal_result(
            directory,
            owned_source,
            confirmed=admission.confirmed,
            catalog=catalog,
            certificates=admission.certificates,
            on_storage_close=resources.close,
        )
        transferred = True
        try:
            if result.anchor != admission.anchor:
                raise LostJournalValidationWorkspace(
                    "sealed native result lost its terminal anchor"
                )
            return result
        except BaseException:
            result.close()
            raise
    except JournalValidationCapacity:
        raise
    except sqlite3.OperationalError as error:
        if error.sqlite_errorcode in {
            sqlite3.SQLITE_BUSY,
            sqlite3.SQLITE_LOCKED,
            sqlite3.SQLITE_FULL,
            sqlite3.SQLITE_NOMEM,
        }:
            raise JournalValidationCapacity(
                "sealed native result resources are unavailable"
            ) from error
        raise LostJournalValidationWorkspace("sealed native result is unavailable") from error
    except (RuntimeError, ValueError, sqlite3.DatabaseError) as error:
        raise LostJournalValidationWorkspace("sealed native result is unavailable") from error
    finally:
        if not transferred:
            resources.close()


declare_growth_for_sealed_admission(prepare_upload_journal_result, SealedJournalResultAdmission)
