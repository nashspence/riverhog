"""Operator-owned plaintext ingress and retained-input capacity."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ArchiveInputGrants:
    """Independent input custody, including the temporary contiguous-copy charge."""

    inputs: int = 16
    inputs_per_target: int = 4
    plaintext_bytes: int = 1280 * 1024 * 1024
    plaintext_bytes_per_target: int = 640 * 1024 * 1024

    def __post_init__(self) -> None:
        for name in (
            "inputs",
            "inputs_per_target",
            "plaintext_bytes",
            "plaintext_bytes_per_target",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError(f"archive input {name} must be a positive integer")
