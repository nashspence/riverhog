from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from riverhog_protocol.errors import ServiceUnavailable

from riverhog_core.ports.archive_objects import (
    ArchiveObjectRangeStore,
    ArchiveResumableObjectStore,
    ImmutableArchiveObjectStore,
)
from riverhog_core.ports.archive_store import ArchiveStore


@dataclass(frozen=True, slots=True)
class ArchiveStoreBinding:
    """All capabilities of one configured archive store."""

    incarnation_id: str
    store: ArchiveStore
    resumable_objects: ArchiveResumableObjectStore
    immutable_objects: ImmutableArchiveObjectStore
    object_ranges: ArchiveObjectRangeStore


class ArchiveStoreRegistry:
    def __init__(
        self,
        stores: Mapping[str, ArchiveStoreBinding],
        *,
        unavailable: Mapping[str, str] | None = None,
        probes: Mapping[str, Callable[[], None]] | None = None,
        recover: Callable[[str | None], None] | None = None,
    ) -> None:
        self._stores = dict(stores)
        self._unavailable = dict(unavailable or {})
        self._probes = dict(probes or {})
        self._recover = recover

    def set_recovery(self, recover: Callable[[str | None], None]) -> None:
        self._recover = recover

    def admit(
        self,
        name: str,
        binding: ArchiveStoreBinding,
        *,
        probe: Callable[[], None],
    ) -> None:
        self._probes = {**self._probes, name: probe}
        self._stores = {**self._stores, name: binding}
        self._unavailable.pop(name, None)

    @property
    def names(self) -> tuple[str, ...]:
        if self._recover is not None:
            self._recover(None)
        return tuple(name for name in self._stores if self._usable(name))

    def require(self, name: str) -> ArchiveStoreBinding:
        if name not in self._stores and self._recover is not None:
            self._recover(name)
        try:
            binding = self._stores[name]
        except KeyError as exc:
            if name in self._unavailable:
                raise ServiceUnavailable(f"archive store is unavailable: {name}") from exc
            raise ValueError(f"archive store is not registered: {name}") from exc
        if not self._usable(name):
            raise ServiceUnavailable(f"archive store is unavailable: {name}")
        return binding

    def incarnation_id(self, name: str) -> str:
        return self.require(name).incarnation_id

    def require_incarnation(self, name: str, incarnation_id: str) -> ArchiveStoreBinding:
        binding = self.require(name)
        if binding.incarnation_id != incarnation_id:
            raise ServiceUnavailable(f"archive store incarnation changed: {name}")
        return binding

    def items(self) -> tuple[tuple[str, ArchiveStoreBinding], ...]:
        if self._recover is not None:
            self._recover(None)
        return tuple(
            (name, binding) for name, binding in self._stores.items() if self._usable(name)
        )

    def _usable(self, name: str) -> bool:
        probe = self._probes.get(name)
        if probe is None:
            return True
        try:
            probe()
        except Exception:
            return False
        return True
