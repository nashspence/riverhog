"""Riverhog-owned private codec binding for exact resumable SHA-256 state."""

from riverhog_protocol.checkpoint_sha256 import CheckpointSHA256 as _SHA256Continuation


class CheckpointSHA256(_SHA256Continuation):
    """Bind operational checkpoints to the Riverhog state owner's codec domain."""

    __slots__ = ()
    _CHECKPOINT_FORMAT = "riverhog-private-sha256-checkpoint/v1"


__all__ = ["CheckpointSHA256"]
