"""Archive-owned immutable identity across randomized opaque encodings."""

from __future__ import annotations

from typing import Any

from riverhog_storage_adapter_protocol import ImmutableObjectReceipt, SmallObjectWriteRequest

from riverhog_core.ports.archive_objects import ArchiveObjectIdentityConflict


def immutable_archive_identity(request: SmallObjectWriteRequest) -> dict[str, Any]:
    identity: dict[str, Any] = {
        "kind": "immutable",
        "object_path": request.object_path,
        "content_type": request.content_type,
        "required_identity_assertions": request.required_identity_assertions,
        "placement_policy": request.placement_policy,
        "mode": request.mode,
    }
    # Only the archive owner's complete plaintext assertions allow randomized
    # ciphertext to differ while retaining the same immutable archive identity.
    if (
        not {
            "riverhog-plaintext-bytes",
            "riverhog-plaintext-sha256",
        }
        <= request.required_identity_assertions.keys()
    ):
        identity.update(stored_bytes=request.stored_bytes, stored_sha256=request.stored_sha256)
    return identity


def require_immutable_archive_receipt(
    request: SmallObjectWriteRequest, receipt: ImmutableObjectReceipt
) -> None:
    """Verify existing object authority without claiming invocation closure."""
    plaintext = {"riverhog-plaintext-bytes", "riverhog-plaintext-sha256"}
    if (
        receipt.object_path != request.object_path
        or receipt.verified_content_type != request.content_type
        or receipt.verified_identity_assertions != request.required_identity_assertions
        or receipt.verified_placement_policy != request.placement_policy
        or (
            not plaintext <= request.required_identity_assertions.keys()
            and (
                receipt.stored_bytes != request.stored_bytes
                or receipt.stored_sha256 != request.stored_sha256
            )
        )
    ):
        raise ArchiveObjectIdentityConflict("existing immutable archive object identity differs")
