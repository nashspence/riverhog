"""Source-owned projection of accepted upload custody counters."""

from __future__ import annotations


def upload_custody_payload(
    *, files: int, byte_count: int, custodied_files: int, custodied_bytes: int
) -> dict[str, object]:
    """Project exact completion separately from progress counters."""
    if (
        custodied_files < 0
        or custodied_bytes < 0
        or custodied_files > files
        or custodied_bytes > byte_count
        or (custodied_files == 0 and custodied_bytes != 0)
    ):
        raise RuntimeError("collection upload custody projection is inconsistent")
    if (custodied_files, custodied_bytes) == (files, byte_count):
        return {"state": "complete"}
    return {"state": "pending", "files": custodied_files, "bytes": custodied_bytes}
