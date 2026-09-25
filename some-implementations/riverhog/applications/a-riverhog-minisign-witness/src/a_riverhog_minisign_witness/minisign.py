"""Subprocess adapter for a caller-supplied Minisign key pair."""

from __future__ import annotations

import base64
import hashlib
import os
import subprocess
import tempfile
from pathlib import Path

from a_riverhog_minisign_witness.store import Signature, SignerError


def public_key_identity(path: Path) -> str:
    """Hash the decoded Minisign public key, independent of its file comment."""
    try:
        lines = path.read_text(encoding="ascii").splitlines()
        if len(lines) != 2 or not lines[0].startswith("untrusted comment: "):
            raise ValueError("invalid public key envelope")
        raw = base64.b64decode(lines[1], validate=True)
        if len(raw) != 42 or raw[:2] != b"Ed":
            raise ValueError("unsupported public key")
    except (OSError, UnicodeError, ValueError) as exc:
        raise SignerError("key_unavailable", retryable=False) from exc
    return "sha256:" + hashlib.sha256(raw).hexdigest()


class MinisignSigner:
    def __init__(
        self,
        secret_key: Path,
        public_key: Path,
        *,
        executable: str = "minisign",
    ) -> None:
        self.secret_key = Path(secret_key)
        self.public_key = Path(public_key)
        self.executable = executable
        mode = self.secret_key.stat().st_mode
        if mode & 0o077:
            raise SignerError("key_unavailable", retryable=False)
        self.key_identity = public_key_identity(self.public_key)

    def sign(self, statement: bytes) -> Signature:
        if not statement:
            raise ValueError("statement is empty")
        with tempfile.TemporaryDirectory(prefix="riverhog-minisign-") as temporary:
            root = Path(temporary)
            message = root / "statement"
            output = root / "statement.minisig"
            message.write_bytes(statement)
            os.chmod(message, 0o600)
            try:
                subprocess.run(
                    [
                        self.executable,
                        "-S",
                        "-W",
                        "-m",
                        str(message),
                        "-s",
                        str(self.secret_key),
                        "-x",
                        str(output),
                        "-t",
                        "a-riverhog-collection-witness/v1",
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=True,
                    timeout=30,
                )
                subprocess.run(
                    [
                        self.executable,
                        "-Vm",
                        str(message),
                        "-p",
                        str(self.public_key),
                        "-x",
                        str(output),
                        "-q",
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=True,
                    timeout=30,
                )
                signature = output.read_bytes()
            except (OSError, subprocess.TimeoutExpired, subprocess.CalledProcessError) as exc:
                raise SignerError("unavailable", retryable=True) from exc
        if len(signature) > 4096 or not signature:
            raise SignerError("invalid_signature", retryable=False)
        return Signature(signature, self.key_identity)
