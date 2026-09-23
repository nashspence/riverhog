"""Filesystem-backed Riverhog archive and retrieval store."""

from a_riverhog_filesystem_store.adapter import (
    FilesystemStorageAdapter,
    FilesystemStorageAdapterConfig,
)

__all__ = ["FilesystemStorageAdapter", "FilesystemStorageAdapterConfig"]
