"""Resolve durable archive placement choices before accepting payload work."""

from __future__ import annotations

import hashlib

from riverhog_protocol.errors import BadRequest

from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.ports.archive_objects import ResumableWriteConstraints
from riverhog_core.ports.retrieval_cache import RetrievalCache
from riverhog_core.runtime_config import RuntimeConfig


def archive_binding_sha256(config: RuntimeConfig, store_name: str) -> str:
    """Fence a configured store name to its adapter endpoint for a pending handoff."""

    registration = config.archive_store(store_name)
    return hashlib.sha256(
        f"riverhog-archive-binding/v1\0{registration.name}\0{registration.base_url}".encode()
    ).hexdigest()


def resolve_use_cache(
    *,
    requested: bool | None,
    store_name: str,
    config: RuntimeConfig,
    archive_stores: ArchiveStoreRegistry,
    retrieval_cache: RetrievalCache | None,
) -> bool:
    if requested is not None and not isinstance(requested, bool):
        raise BadRequest("use_cache must be a boolean")
    binding = archive_stores.require(store_name)
    resolved = (
        requested
        if requested is not None
        else config.retrieval_cache_new_archive_enabled
        and binding.store.read_mode() == "restore_required"
    )
    if resolved and retrieval_cache is None:
        if requested is not None:
            raise BadRequest("use_cache requires a configured retrieval cache")
        return False
    if resolved and retrieval_cache is not None:
        cache_constraints = getattr(retrieval_cache, "mirror_write_constraints", None)
        if callable(cache_constraints):
            common = cache_constraints(binding.resumable_objects.write_constraints())
            if common is None:
                raise BadRequest("archive and retrieval cache write constraints are incompatible")
    return resolved


def common_write_constraints(
    archive: ResumableWriteConstraints,
    caches: tuple[ResumableWriteConstraints, ...],
) -> ResumableWriteConstraints | None:
    if not caches:
        return None
    minimum = max(
        (archive.minimum_nonfinal_segment_bytes,)
        + tuple(current.minimum_nonfinal_segment_bytes for current in caches)
    )
    maximum_values = (archive.maximum_segment_bytes,) + tuple(
        current.maximum_segment_bytes for current in caches
    )
    maximum = min((value for value in maximum_values if value is not None), default=None)
    counts = (archive.maximum_segment_count,) + tuple(
        current.maximum_segment_count for current in caches
    )
    count = min((value for value in counts if value is not None), default=None)
    if maximum is not None and minimum > maximum:
        return None
    return ResumableWriteConstraints(minimum, maximum, count)
