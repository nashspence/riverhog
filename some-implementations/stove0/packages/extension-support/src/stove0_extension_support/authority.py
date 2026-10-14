"""Read-only live Riverhog authority check before component payload dispatch."""

from __future__ import annotations

import time

from riverhog_client import ApiClient
from riverhog_protocol.collection_workflows import CollectionRootIdentity


def validate_live_root_authority(
    *,
    base_url: str,
    capability_token: str,
    allow_insecure_http: bool,
    root: CollectionRootIdentity,
    deadline: float,
) -> None:
    """Require a current capability and its exact selected collection root.

    Riverhog authenticates the live capability and claim fence on this ordinary
    catalog read. The descriptor establishes no output or completion authority.
    This call creates neither a retrieval nor a plaintext workspace.
    """
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("extension authority check deadline expired")
    api = ApiClient(
        base_url=base_url, token=capability_token, allow_insecure_http=allow_insecure_http
    )
    api.timeout_seconds = remaining
    try:
        record = api.get_collection(root.collection_id)
        if (
            record.get("archive_root_sha256") != root.archive_root_sha256
            or record.get("artifact_set_identity") != root.artifact_set_identity
            or time.monotonic() >= deadline
        ):
            raise PermissionError("extension input root authority is unavailable")
    finally:
        api.close()


__all__ = ["validate_live_root_authority"]
