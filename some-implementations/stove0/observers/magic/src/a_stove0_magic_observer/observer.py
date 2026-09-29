"""Observe bounded payload bytes with one pinned file/libmagic installation."""

from __future__ import annotations

import hashlib
import importlib.metadata
import subprocess
from pathlib import Path
from typing import cast

from a_stove0_magic_facts_contract_lib import (
    MAGIC_OBSERVER_CONTRACT,
    validate_magic_facts,
)
from a_stove0_magic_facts_contract_lib.contracts import MagicOptions
from pydantic import JsonValue
from stove0_observer_protocol import (
    ContentObservationRequest,
    ContentObservationResult,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
)
from stove0_observer_support import ContentObservationResultBuilder, ContentObservationRuntime


def _digest_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(8 * 1024**2):
            digest.update(block)
    return digest.hexdigest()


class FileMagic:
    def __init__(self, executable: Path, database: Path) -> None:
        self.executable = executable.resolve(strict=True)
        self.database = database.resolve(strict=True)
        self.executable_sha256 = _digest_file(self.executable)
        self.database_sha256 = _digest_file(self.database)
        result = subprocess.run(
            [str(self.executable), "--version"],
            check=True,
            capture_output=True,
            timeout=5,
            env={"LC_ALL": "C"},
        )
        version = result.stdout.decode("utf-8", "strict").splitlines()
        if not version or not version[0].strip() or len(version[0]) > 200:
            raise ValueError("file tool omitted a bounded version identity")
        self.version = version[0].strip()

    def verify(self) -> None:
        if (
            _digest_file(self.executable) != self.executable_sha256
            or _digest_file(self.database) != self.database_sha256
        ):
            raise ValueError("file/libmagic installation changed after observer startup")

    def probe(self, sample: bytes, *, timeout_seconds: int) -> tuple[str, str]:
        self.verify()
        values = []
        for mode in (("--mime-type",), ()):
            result = subprocess.run(
                [
                    str(self.executable),
                    "--brief",
                    *mode,
                    "-m",
                    str(self.database),
                    "-P",
                    f"bytes={max(1, len(sample))}",
                    "-",
                ],
                input=sample,
                check=True,
                capture_output=True,
                timeout=timeout_seconds,
                env={"LC_ALL": "C"},
            )
            if result.stderr or len(result.stdout) > 4096:
                raise ValueError("file/libmagic returned unexpected or oversized output")
            value = result.stdout.decode("utf-8", "strict").rstrip("\n")
            if not value:
                raise ValueError("file/libmagic returned no classification")
            values.append(value)
        return values[0], values[1]


def _version() -> str:
    try:
        return importlib.metadata.version("a-stove0-magic-observer")
    except importlib.metadata.PackageNotFoundError:
        return "development"


class MagicObserver:
    def __init__(
        self, engine: FileMagic, *, image_id: str, source_revision: str = "unknown"
    ) -> None:
        self.engine = engine
        self._descriptor = ObserverDescriptor.seal(
            ObserverDescriptorPayload(
                implementation_id="a-stove0-magic-observer/v1",
                implementation_version=_version(),
                source_revision=source_revision,
                image_id=image_id,
                contracts=(
                    ObserverContractSupport.from_contract(
                        MAGIC_OBSERVER_CONTRACT, preferred_subject_batch_size=1
                    ),
                ),
            )
        )

    def descriptor(self) -> ObserverDescriptor:
        return self._descriptor

    def observe(
        self, request: ContentObservationRequest, runtime: ContentObservationRuntime
    ) -> ContentObservationResult:
        builder = ContentObservationResultBuilder(self._descriptor, request)
        try:
            options = MagicOptions.model_validate(request.options)
            facts = []
            for subject in request.subjects:
                runtime.heartbeat()
                wanted = min(int(subject.bytes), options.maximum_prefix_bytes)
                sample = bytearray()
                with runtime.stream(subject, start=0, end=wanted) as chunks:
                    for chunk in chunks:
                        if len(chunk) > wanted - len(sample):
                            raise ValueError(
                                "authorized payload sample exceeded its requested extent"
                            )
                        sample.extend(chunk)
                if len(sample) != wanted:
                    raise ValueError("authorized payload sample ended before the requested extent")
                sample_sha256 = hashlib.sha256(sample).hexdigest()
                if wanted == int(subject.bytes) and sample_sha256 != subject.sha256:
                    raise ValueError("complete authorized payload differs from selected fixity")
                mime_type, description = self.engine.probe(
                    bytes(sample),
                    timeout_seconds=min(options.timeout_seconds, request.timeout_seconds),
                )
                facts.append(
                    {
                        "subject_id": subject.id,
                        "mime_type": mime_type,
                        "description": description,
                        "sampled_bytes": wanted,
                        "sample_sha256": sample_sha256,
                        "complete_payload": wanted == int(subject.bytes),
                        "file_version": self.engine.version,
                        "executable_sha256": self.engine.executable_sha256,
                        "database_sha256": self.engine.database_sha256,
                    }
                )
            document = {"artifacts": facts}
            validated = validate_magic_facts(document, request.subjects, request.options)
            return builder.observed(cast(dict[str, JsonValue], validated.model_dump(mode="json")))
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
            return builder.failed(
                code="magic-observation-failed",
                message="Bounded byte classification could not be completed.",
                retryable=isinstance(exc, (RuntimeError, OSError, subprocess.TimeoutExpired)),
            )


__all__ = ["FileMagic", "MagicObserver"]
