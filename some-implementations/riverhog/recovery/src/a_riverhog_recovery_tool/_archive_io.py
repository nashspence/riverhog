"""Bounded encrypted-object reads for independent offline recovery."""

from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path, PurePosixPath


def archive_file(root: Path, relative: str) -> Path:
    parts = PurePosixPath(relative).parts
    if (
        not relative
        or relative.startswith("/")
        or "\\" in relative
        or any(part in {"", ".", ".."} for part in parts)
        or str(PurePosixPath(relative)) != relative
    ):
        raise ValueError("archive object has an unsafe relative path")
    current = root
    for part in parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"archive object traverses a symbolic link: {relative}")
    if not current.is_file():
        raise ValueError(f"archive object is missing: {relative}")
    return current


def sha256_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    byte_count = 0
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            byte_count += len(chunk)
            digest.update(chunk)
    return byte_count, digest.hexdigest()


def age_decrypt(source: Path, destination: Path, *, passphrase: str, command: str) -> None:
    """Use the official batchpass plugin without a command-line secret."""

    environment = os.environ.copy()
    environment.pop("AGE_PASSPHRASE", None)
    environment.pop("AGE_PASSPHRASE_FD", None)
    read_fd: int | None = None
    if os.name == "nt":
        environment["AGE_PASSPHRASE"] = passphrase
    else:
        read_fd, write_fd = os.pipe()
        try:
            os.write(write_fd, passphrase.encode("utf-8"))
        finally:
            os.close(write_fd)
        environment["AGE_PASSPHRASE_FD"] = str(read_fd)
    try:
        arguments = [command, "--decrypt", "-j", "batchpass", "-o", str(destination), str(source)]
        if read_fd is None:
            completed = subprocess.run(
                arguments, check=False, capture_output=True, text=True, env=environment
            )
        else:
            completed = subprocess.run(
                arguments,
                check=False,
                capture_output=True,
                text=True,
                env=environment,
                pass_fds=(read_fd,),
            )
    finally:
        if read_fd is not None:
            os.close(read_fd)
    if completed.returncode != 0 or not destination.is_file():
        raise ValueError(f"age decryption failed for {source.name}")


class EncryptedArchive:
    def __init__(self, root: Path, *, passphrase: str, age_command: str, scratch: Path) -> None:
        self.root = root
        self.passphrase = passphrase
        self.age_command = age_command
        self.scratch = scratch
        self.scratch.mkdir(parents=True, exist_ok=True)

    def decrypt_to(self, relative: str, destination: Path) -> Path:
        source = archive_file(self.root, relative)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=destination.parent, prefix=".decrypt-", delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
        try:
            age_decrypt(
                source,
                temporary_path,
                passphrase=self.passphrase,
                command=self.age_command,
            )
            os.replace(temporary_path, destination)
        finally:
            temporary_path.unlink(missing_ok=True)
        return destination

    def read_bounded(self, relative: str, maximum: int) -> bytes:
        if maximum < 0:
            raise ValueError("archive read bound is invalid")
        with tempfile.TemporaryDirectory(dir=self.scratch, prefix=".read-") as directory:
            path = self.decrypt_to(relative, Path(directory) / "plaintext")
            if path.stat().st_size > maximum:
                raise ValueError(f"archive object exceeds its bound: {relative}")
            return path.read_bytes()

    def iter_plaintext(self, relative: str) -> Iterator[bytes]:
        with tempfile.TemporaryDirectory(dir=self.scratch, prefix=".stream-") as directory:
            path = self.decrypt_to(relative, Path(directory) / "plaintext")
            with path.open("rb") as source:
                while chunk := source.read(8 * 1024 * 1024):
                    yield chunk


__all__ = ["EncryptedArchive", "age_decrypt", "archive_file", "sha256_file"]
