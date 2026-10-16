"""Source and sealed-catalog bindings for the private native replay handoff."""

from __future__ import annotations

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance.replay_binding import JournalReplayBinding
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import and_, select, update
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import CollectionUploadProvenanceEpochScanRecord as Scan
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal

from . import collection_uploads as source_owner
from . import upload_journal_rank as rank


def _binding(
    source: source_owner._UploadJournalValidationWork, catalog: ContractCatalog
) -> JournalReplayBinding:
    return JournalReplayBinding(
        attempt_id=source.attempt_id,
        directory_id=source.directory_id,
        journal_id=source.journal_id,
        source_bytes=str(source.source_bytes),
        source_sha256=source.source_sha256,
        source_version=str(source.version),
        frame_count=str(source.cursor.frame_count),
        catalog_sha256=catalog.checkpoint_sha256(),
        require_profiles=False,
    )


def _commit_binding(
    factory: SessionFactory, work: rank._Work, binding: JournalReplayBinding
) -> bool:
    with factory() as session, session.begin():
        if (
            session.scalar(
                select(Journal.collection_id)
                .where(*source_owner._upload_journal_validation_fence(work.source))
                .with_for_update(skip_locked=True)
            )
            is None
        ):
            return False
        with session.execute(select(Scan.replay_binding_json).where(*rank._fence(work))) as result:
            row = result.one_or_none()
        if row is None:
            return False
        if row[0] is not None:
            if JournalReplayBinding.from_json(row[0]) != binding:
                raise RuntimeError("retained replay inputs differ from the selected attempt")
            return True
        changed = session.execute(
            update(Scan)
            .where(*rank._fence(work), Scan.replay_binding_json.is_(None))
            .values(version=work.parent.version + 1, replay_binding_json=binding.to_json())
            .returning(Scan.collection_id)
        ).scalar_one_or_none()
        return changed is not None


def prepare_upload_journal_replay(
    factory: SessionFactory,
    source: source_owner._UploadJournalValidationWork,
    *,
    catalog: ContractCatalog,
) -> bool:
    binding = _binding(source, catalog)
    work = rank._begin(factory, source)
    if work is None:
        return False
    if work.state == "deferred":
        return True
    if work.state != "complete":
        return False
    return _commit_binding(factory, work, binding)


@declare_growth(
    Declaration(
        id="riverhog-server.sqlite-journal-replay-binding.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.services.upload_journal_replay._probe",),
        scope="native-sqlite-exact-source-and-retained-replay-binding-point-probe",
        starts_at="probe the exact current journal and its attempt-qualified replay metadata",
        ends_at="return detached current/binding metadata after closing snapshot resources",
        dimensions=(
            Dimension(
                "retained_journals", COUNT, "Journal and optional private scan rows in the fixture."
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=1, unit=COUNT),
                "One source-fenced primary-key lookup with a primary-key scan join.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=1, unit=COUNT),
                "The exact journal key returns at most one metadata row.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No staged journal octets or schema payloads enter this probe.",
            ),
            Bound(
                "binding_octets",
                OCTETS,
                "upper",
                Expression("constant", value=2048, unit=OCTETS),
                "The owning fixed-shape binding contains UUIDs, digests, mode and source scalars.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=256, unit=COUNT),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=64, unit=COUNT),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("constant", value=2, unit=COUNT),
                                                Expression("dimension", name="retained_journals"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Native instruction envelope for the two exact primary-key searches.",
            ),
        ),
        conditions=(
            "The exact source/job fence and owning primary keys are present.",
            "Binding records are owner-created with native UUID/digest domains "
            "and owning source scalars.",
        ),
        invariants=(
            "No source or catalog binding accepts a semantic prefix.",
            "A source replacement differs from an unprepared but current source.",
        ),
        limitations=(
            "Catalog initialization, decoding, source capture, native semantics, "
            "publication and cleanup are separate costs.",
            "SQLite instructions exclude internal key comparisons, page I/O and CPU; "
            "other engines need separate qualification.",
        ),
    )
)
def _probe(
    factory: SessionFactory, source: source_owner._UploadJournalValidationWork
) -> tuple[bool, str | None]:
    query = (
        select(Scan.replay_binding_json)
        .select_from(Journal)
        .outerjoin(
            Scan,
            and_(
                Scan.collection_id == Journal.collection_id,
                Scan.journal_id == Journal.journal_id,
                Scan.attempt_id == source.attempt_id,
                Scan.directory_id == source.directory_id,
            ),
        )
        .where(*source_owner._upload_journal_validation_fence(source))
    )
    with read_snapshot(factory) as session:
        with session.execute(query) as result:
            row = result.one_or_none()
    return (row is not None, None if row is None else row[0])


def verify_upload_journal_replay_binding(
    factory: SessionFactory,
    source: source_owner._UploadJournalValidationWork,
    catalog: ContractCatalog,
) -> bool:
    current, raw = _probe(factory, source)
    if raw is not None and JournalReplayBinding.from_json(raw) != _binding(source, catalog):
        raise RuntimeError("retained replay inputs differ from the selected attempt")
    return current
