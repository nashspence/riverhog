"""Executable reference model for Riverhog issue #951.

This module is intentionally independent of Riverhog production imports.  It models the
selection invariants proposed for v1: archive fallback selection remains copy/incarnation
fenced, while an already-ready equivalent cache placement may be reused across archive
source names and is pinned separately from the archive fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ReadMode = Literal["immediate", "restore_required"]
PlannedReadMode = Literal["cache", "immediate", "restore_required"]


class SelectionError(RuntimeError):
    """The requested/default archive fallback cannot be selected."""


class InvariantViolation(RuntimeError):
    """Supposedly equivalent archive copies disagree on sealed payload identity."""


@dataclass(frozen=True, slots=True)
class PayloadIdentity:
    """Store-independent sealed identity for one cached payload object.

    Production Riverhog can derive this comparison from existing catalog state rather than
    introducing a new public identity.  ``age_state_sha256`` represents canonicalized age
    state.  Pack fields bind the canonical pack recipe; segment fields bind the exact source
    artifact slice.  Provider path/revision and provider write segmentation are deliberately
    excluded.
    """

    object_id: str
    kind: Literal["pack", "segment"]
    plaintext_bytes: int
    stored_bytes: int
    age_state_sha256: str
    pack_plan_sha256: str | None = None
    pack_index_sha256: str | None = None
    segment_artifact_id: str | None = None
    segment_artifact_bytes: int | None = None
    segment_artifact_sha256: str | None = None
    segment_artifact_offset: int | None = None
    segment_bytes: int | None = None

    def __post_init__(self) -> None:
        if self.plaintext_bytes < 1 or self.stored_bytes < 1:
            raise ValueError("payload byte counts must be positive")
        if self.kind == "pack":
            if not self.pack_plan_sha256 or not self.pack_index_sha256:
                raise ValueError("pack payload identity requires plan and index identities")
            if any(
                value is not None
                for value in (
                    self.segment_artifact_id,
                    self.segment_artifact_bytes,
                    self.segment_artifact_sha256,
                    self.segment_artifact_offset,
                    self.segment_bytes,
                )
            ):
                raise ValueError("pack payload identity cannot carry segment fields")
        else:
            if self.pack_plan_sha256 is not None or self.pack_index_sha256 is not None:
                raise ValueError("segment payload identity cannot carry pack fields")
            if any(
                value is None
                for value in (
                    self.segment_artifact_id,
                    self.segment_artifact_bytes,
                    self.segment_artifact_sha256,
                    self.segment_artifact_offset,
                    self.segment_bytes,
                )
            ):
                raise ValueError("segment payload identity requires exact artifact-slice identity")


@dataclass(frozen=True, slots=True)
class ArchiveObject:
    identity: PayloadIdentity


@dataclass(frozen=True, slots=True)
class ArchiveCopy:
    store: str
    incarnation_id: str
    read_mode: ReadMode
    complete: bool = True
    retiring: bool = False
    usable_binding: bool = True
    objects: tuple[ArchiveObject, ...] = ()

    def object(self, object_id: str) -> ArchiveObject:
        matches = [current for current in self.objects if current.identity.object_id == object_id]
        if len(matches) != 1:
            raise InvariantViolation(
                f"archive copy {self.store!r} does not have exactly one payload object {object_id!r}"
            )
        return matches[0]


@dataclass(frozen=True, slots=True)
class CachePlacement:
    collection_id: int
    object_id: str
    source_store: str
    source_incarnation_id: str
    cache_store: str
    cache_incarnation_id: str
    ready: bool = True
    usable_cache_binding: bool = True


@dataclass(frozen=True, slots=True)
class SelectionPolicy:
    archive_read_order: tuple[str, ...]
    cache_store_order: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PlannedObject:
    object_id: str
    archive_source_store: str
    archive_source_incarnation_id: str
    read_mode: PlannedReadMode
    cache_source_store: str | None
    cache_source_incarnation_id: str | None
    cache_store: str | None
    cache_incarnation_id: str | None


@dataclass(frozen=True, slots=True)
class RetrievalPlan:
    requested_source_store: str | None
    archive_source_store: str
    archive_source_incarnation_id: str
    objects: tuple[PlannedObject, ...]

    @property
    def requires_restore(self) -> bool:
        return any(current.read_mode == "restore_required" for current in self.objects)


def _copy_by_store(copies: tuple[ArchiveCopy, ...], store: str) -> ArchiveCopy | None:
    matches = [current for current in copies if current.store == store]
    if len(matches) > 1:
        raise InvariantViolation(f"multiple durable copies use archive store name {store!r}")
    return matches[0] if matches else None


def select_archive_source(
    copies: tuple[ArchiveCopy, ...],
    *,
    policy: SelectionPolicy,
    requested_source_store: str | None,
) -> ArchiveCopy:
    """Select one strict archive fallback before considering cache placements."""

    def eligible(copy: ArchiveCopy | None) -> bool:
        return bool(
            copy is not None
            and copy.complete
            and not copy.retiring
            and copy.usable_binding
        )

    if requested_source_store is not None:
        requested = _copy_by_store(copies, requested_source_store)
        if not eligible(requested):
            raise SelectionError(
                f"requested archive source is not a usable complete copy: {requested_source_store}"
            )
        assert requested is not None
        return requested

    for store in policy.archive_read_order:
        candidate = _copy_by_store(copies, store)
        if eligible(candidate):
            assert candidate is not None
            return candidate
    raise SelectionError("collection has no readable archive copy")


def _cache_rank(cache_store: str, order: tuple[str, ...]) -> tuple[int, str]:
    try:
        return order.index(cache_store), cache_store
    except ValueError:
        return len(order), cache_store


def select_equivalent_cache(
    *,
    collection_id: int,
    selected_object: ArchiveObject,
    copies: tuple[ArchiveCopy, ...],
    placements: tuple[CachePlacement, ...],
    policy: SelectionPolicy,
) -> CachePlacement | None:
    """Choose a ready equivalent cache placement across archive source names.

    A cache placement retains historical source-copy provenance.  Its source archive binding
    does not have to be currently reachable because no archive read is performed through it,
    but the durable source copy/object must still exist, remain complete/non-retiring, and
    match the selected archive object's copy-invariant payload identity exactly.
    """

    candidates = sorted(
        (
            current
            for current in placements
            if current.collection_id == collection_id
            and current.object_id == selected_object.identity.object_id
            and current.ready
            and current.usable_cache_binding
        ),
        key=lambda current: (
            *_cache_rank(current.cache_store, policy.cache_store_order),
            current.source_store,
            current.source_incarnation_id,
            current.cache_incarnation_id,
        ),
    )
    for current in candidates:
        source_copy = _copy_by_store(copies, current.source_store)
        if (
            source_copy is None
            or source_copy.incarnation_id != current.source_incarnation_id
            or not source_copy.complete
            or source_copy.retiring
        ):
            continue
        source_object = source_copy.object(current.object_id)
        if source_object.identity != selected_object.identity:
            raise InvariantViolation(
                "complete archive copies disagree on copy-invariant sealed payload identity"
            )
        return current
    return None


def plan_retrieval(
    *,
    collection_id: int,
    copies: tuple[ArchiveCopy, ...],
    placements: tuple[CachePlacement, ...],
    object_ids: tuple[str, ...],
    policy: SelectionPolicy,
    requested_source_store: str | None = None,
) -> RetrievalPlan:
    archive = select_archive_source(
        copies,
        policy=policy,
        requested_source_store=requested_source_store,
    )
    planned: list[PlannedObject] = []
    for object_id in object_ids:
        selected_object = archive.object(object_id)
        cached = select_equivalent_cache(
            collection_id=collection_id,
            selected_object=selected_object,
            copies=copies,
            placements=placements,
            policy=policy,
        )
        if cached is not None:
            planned.append(
                PlannedObject(
                    object_id=object_id,
                    archive_source_store=archive.store,
                    archive_source_incarnation_id=archive.incarnation_id,
                    read_mode="cache",
                    cache_source_store=cached.source_store,
                    cache_source_incarnation_id=cached.source_incarnation_id,
                    cache_store=cached.cache_store,
                    cache_incarnation_id=cached.cache_incarnation_id,
                )
            )
        else:
            planned.append(
                PlannedObject(
                    object_id=object_id,
                    archive_source_store=archive.store,
                    archive_source_incarnation_id=archive.incarnation_id,
                    read_mode=archive.read_mode,
                    cache_source_store=None,
                    cache_source_incarnation_id=None,
                    cache_store=None,
                    cache_incarnation_id=None,
                )
            )
    return RetrievalPlan(
        requested_source_store=requested_source_store,
        archive_source_store=archive.store,
        archive_source_incarnation_id=archive.incarnation_id,
        objects=tuple(planned),
    )
