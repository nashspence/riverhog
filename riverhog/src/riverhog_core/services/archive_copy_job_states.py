from __future__ import annotations

from http_api_contracts import closed_literal_values
from riverhog_protocol import ArchiveCopyJobState

ARCHIVE_COPY_JOB_TRANSFER_STATES = frozenset({"requested", "waiting", "checking", "copying"})
ARCHIVE_COPY_JOB_BLOCKING_STATES = ARCHIVE_COPY_JOB_TRANSFER_STATES | {"canceling"}
ARCHIVE_COPY_JOB_STATES = closed_literal_values(ArchiveCopyJobState)

__all__ = [
    "ARCHIVE_COPY_JOB_BLOCKING_STATES",
    "ARCHIVE_COPY_JOB_STATES",
    "ARCHIVE_COPY_JOB_TRANSFER_STATES",
]
