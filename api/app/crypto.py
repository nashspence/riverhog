from __future__ import annotations

import hashlib
import math
import os
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path
from typing import Iterator

from .config import (
    AGE_BATCHPASS_MAX_WORK_FACTOR,
    AGE_BATCHPASS_PASSPHRASE,
    AGE_BATCHPASS_WORK_FACTOR,
    AGE_CLI,
)

CHUNK_SIZE = 1024 * 1024
AGE_STREAM_CHUNK_SIZE = 64 * 1024
AGE_STREAM_TAG_BYTES = 16
AGE_MAGIC_PREFIXES = (
    b"age-encryption.org/",
    b"-----BEGIN AGE ENCRYPTED FILE-----",
)


class AgeEncryptionError(RuntimeError):
    pass


def _age_env() -> dict[str, str]:
    env = os.environ.copy()
    env["AGE_PASSPHRASE"] = AGE_BATCHPASS_PASSPHRASE
    env["AGE_PASSPHRASE_WORK_FACTOR"] = AGE_BATCHPASS_WORK_FACTOR
    env["AGE_PASSPHRASE_MAX_WORK_FACTOR"] = AGE_BATCHPASS_MAX_WORK_FACTOR
    age_cli = Path(AGE_CLI)
    if age_cli.parent != Path("."):
        env["PATH"] = f"{age_cli.parent}{os.pathsep}{env.get('PATH', '')}"
    return env


def _read_head(path: Path, size: int = 64) -> bytes:
    with path.open("rb") as handle:
        return handle.read(size)


def is_age_encrypted_file(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    head = _read_head(path)
    return any(head.startswith(prefix) for prefix in AGE_MAGIC_PREFIXES)


def _stream_process_to_file(command: list[str], chunks: Iterator[bytes], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp = output_path.with_name(f".{output_path.name}.tmp")
    with temp.open("wb") as output:
        with subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=output,
            stderr=subprocess.PIPE,
            env=_age_env(),
        ) as process:
            assert process.stdin is not None
            assert process.stderr is not None
            try:
                for chunk in chunks:
                    process.stdin.write(chunk)
            except Exception:
                process.kill()
                process.wait()
                temp.unlink(missing_ok=True)
                raise
            finally:
                process.stdin.close()

            stderr = process.stderr.read().decode("utf-8", "replace").strip()
            returncode = process.wait()
            if returncode != 0:
                temp.unlink(missing_ok=True)
                raise AgeEncryptionError(stderr or "age command failed")

    temp.replace(output_path)


@lru_cache(maxsize=1)
def age_ciphertext_overhead_bytes() -> int:
    process = subprocess.run(
        [AGE_CLI, "-e", "-j", "batchpass"],
        input=b"",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=_age_env(),
        check=False,
    )
    if process.returncode != 0:
        message = process.stderr.decode("utf-8", "replace").strip()
        raise AgeEncryptionError(message or "age encrypt failed")
    return len(process.stdout)


def encrypted_size_for_plaintext_size(plaintext_size: int) -> int:
    if plaintext_size <= 0:
        return age_ciphertext_overhead_bytes()
    extra_chunks = max(0, math.ceil(plaintext_size / AGE_STREAM_CHUNK_SIZE) - 1)
    return plaintext_size + age_ciphertext_overhead_bytes() + (extra_chunks * AGE_STREAM_TAG_BYTES)


def max_plaintext_size_for_encrypted_budget(budget: int) -> int:
    if budget <= 0:
        return 0
    low, high = 0, budget
    while low < high:
        mid = (low + high + 1) // 2
        if encrypted_size_for_plaintext_size(mid) <= budget:
            low = mid
        else:
            high = mid - 1
    return low


def _iter_file_span(path: Path, offset: int = 0, size: int | None = None) -> Iterator[bytes]:
    remaining = size
    with path.open("rb") as handle:
        if offset:
            handle.seek(offset)
        while True:
            if remaining is not None and remaining <= 0:
                break
            chunk = handle.read(CHUNK_SIZE if remaining is None else min(CHUNK_SIZE, remaining))
            if not chunk:
                break
            if remaining is not None:
                remaining -= len(chunk)
            yield chunk


def encrypt_bytes_to_file(data: bytes, output_path: Path) -> None:
    _stream_process_to_file(
        [AGE_CLI, "-e", "-j", "batchpass"],
        iter((data,)),
        output_path,
    )


def encrypt_file_span(source_path: Path, output_path: Path, offset: int = 0, size: int | None = None) -> None:
    _stream_process_to_file(
        [AGE_CLI, "-e", "-j", "batchpass"],
        _iter_file_span(source_path, offset=offset, size=size),
        output_path,
    )


def decrypt_file(source_path: Path, output_path: Path) -> None:
    if not is_age_encrypted_file(source_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp = output_path.with_name(f".{output_path.name}.tmp")
        shutil.copy2(source_path, temp)
        temp.replace(output_path)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp = output_path.with_name(f".{output_path.name}.tmp")
    with temp.open("wb") as output:
        result = subprocess.run(
            [AGE_CLI, "-d", "-j", "batchpass", str(source_path)],
            stdout=output,
            stderr=subprocess.PIPE,
            env=_age_env(),
            check=False,
        )
    if result.returncode != 0:
        temp.unlink(missing_ok=True)
        message = result.stderr.decode("utf-8", "replace").strip()
        raise AgeEncryptionError(message or "age decrypt failed")
    temp.replace(output_path)


def logical_file_sha256_and_size(path: Path) -> tuple[str, int]:
    if not is_age_encrypted_file(path):
        digest = hashlib.sha256()
        total = 0
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
                digest.update(chunk)
                total += len(chunk)
        return digest.hexdigest(), total

    with subprocess.Popen(
        [AGE_CLI, "-d", "-j", "batchpass", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=_age_env(),
    ) as process:
        assert process.stdout is not None
        assert process.stderr is not None

        digest = hashlib.sha256()
        total = 0
        for chunk in iter(lambda: process.stdout.read(CHUNK_SIZE), b""):
            digest.update(chunk)
            total += len(chunk)

        stderr = process.stderr.read().decode("utf-8", "replace").strip()
        returncode = process.wait()
        if returncode != 0:
            raise AgeEncryptionError(stderr or "age decrypt failed")
    return digest.hexdigest(), total


def decrypt_tree(source_root: Path, output_root: Path) -> None:
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    for path in sorted(source_root.rglob("*")):
        rel = path.relative_to(source_root)
        target = output_root / rel
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        decrypt_file(path, target)
