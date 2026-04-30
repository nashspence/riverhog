from __future__ import annotations

import hashlib
import os
import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest

from arc_disc import main as arc_disc_main

_FAKE_FACTORY_ENV_VARS = (
    "ARC_DISC_READER_FACTORY",
    "ARC_DISC_ISO_VERIFIER_FACTORY",
    "ARC_DISC_BURNER_FACTORY",
    "ARC_DISC_BURNED_MEDIA_VERIFIER_FACTORY",
    "ARC_DISC_BURN_PROMPTS_FACTORY",
)
_BURN_CONFIRMATION = "write-optical-media"


def _reject_fake_factories() -> None:
    configured = [name for name in _FAKE_FACTORY_ENV_VARS if os.environ.get(name)]
    if configured:
        names = ", ".join(configured)
        pytest.fail(f"gated arc-disc validation must not use fake factory env vars: {names}")


def _required_env(name: str, *, reason: str) -> str:
    value = os.environ.get(name)
    if value:
        return value
    pytest.skip(f"{name} is required for {reason}")


def _optional_env(primary: str, fallback: str, *, reason: str) -> str:
    value = os.environ.get(primary) or os.environ.get(fallback)
    if value:
        return value
    pytest.skip(f"{primary} or {fallback} is required for {reason}")


def _required_path(name: str, *, reason: str) -> Path:
    path = Path(_required_env(name, reason=reason)).expanduser()
    if path.exists():
        return path
    pytest.skip(f"{name} does not exist: {path}")


def _require_xorriso(*, reason: str) -> None:
    if shutil.which("xorriso") is None:
        pytest.skip(f"xorriso is required for {reason}")


def _sha256_chunks(chunks: Iterator[bytes]) -> str:
    digest = hashlib.sha256()
    saw_bytes = False
    for chunk in chunks:
        if not chunk:
            continue
        saw_bytes = True
        digest.update(chunk)
    if not saw_bytes:
        raise AssertionError("arc-disc optical reader returned no bytes")
    return digest.hexdigest()


def _read_recovery_copy_sha256(
    reader: object,
    *,
    disc_path: str,
    device: str,
) -> str:
    copy = arc_disc_main.RecoveryCopyHint(
        copy_id="gated-arc-disc-copy",
        location="gated validation media",
        disc_path=disc_path,
        recovery_bytes=0,
        recovery_sha256="",
    )
    return _sha256_chunks(arc_disc_main._iter_recovered_chunks(reader, copy, device=device))


def test_default_reader_recovers_configured_mounted_media_payload() -> None:
    _reject_fake_factories()
    mount_path = _required_path(
        "ARC_DISC_GATED_MOUNT_PATH",
        reason="mounted optical-media recovery validation",
    )
    if not mount_path.is_dir():
        pytest.skip(f"ARC_DISC_GATED_MOUNT_PATH must be a mounted directory: {mount_path}")
    disc_path = _required_env(
        "ARC_DISC_GATED_PAYLOAD_PATH",
        reason="mounted optical-media recovery validation",
    )
    expected_sha256 = _required_env(
        "ARC_DISC_GATED_EXPECTED_SHA256",
        reason="mounted optical-media recovery validation",
    )

    reader = arc_disc_main.build_optical_reader()
    if not isinstance(reader, arc_disc_main.XorrisoOpticalReader):
        raise AssertionError(f"default reader is not the real backend: {type(reader).__name__}")

    actual_sha256 = _read_recovery_copy_sha256(
        reader,
        disc_path=disc_path,
        device=str(mount_path),
    )

    assert actual_sha256 == expected_sha256


def test_default_reader_recovers_configured_raw_device_payload() -> None:
    _reject_fake_factories()
    _require_xorriso(reason="raw optical-device recovery validation")
    device = _required_path(
        "ARC_DISC_GATED_RAW_DEVICE",
        reason="raw optical-device recovery validation",
    )
    if not os.access(device, os.R_OK):
        pytest.skip(f"ARC_DISC_GATED_RAW_DEVICE is not readable by this user: {device}")
    disc_path = _optional_env(
        "ARC_DISC_GATED_RAW_PAYLOAD_PATH",
        "ARC_DISC_GATED_PAYLOAD_PATH",
        reason="raw optical-device recovery validation",
    )
    expected_sha256 = _optional_env(
        "ARC_DISC_GATED_RAW_EXPECTED_SHA256",
        "ARC_DISC_GATED_EXPECTED_SHA256",
        reason="raw optical-device recovery validation",
    )

    reader = arc_disc_main.build_optical_reader()
    if not isinstance(reader, arc_disc_main.XorrisoOpticalReader):
        raise AssertionError(f"default reader is not the real backend: {type(reader).__name__}")

    actual_sha256 = _read_recovery_copy_sha256(
        reader,
        disc_path=disc_path,
        device=str(device),
    )

    assert actual_sha256 == expected_sha256


def test_destructive_burn_backend_writes_and_verifies_configured_media() -> None:
    _reject_fake_factories()
    _require_xorriso(reason="destructive optical burn validation")
    if os.environ.get("ARC_DISC_GATED_BURN_CONFIRM") != _BURN_CONFIRMATION:
        pytest.skip(
            "set ARC_DISC_GATED_BURN_CONFIRM=write-optical-media to run destructive burn "
            "validation"
        )
    device = _required_path(
        "ARC_DISC_GATED_BURN_DEVICE",
        reason="destructive optical burn validation",
    )
    iso_path = _required_path(
        "ARC_DISC_GATED_BURN_ISO_PATH",
        reason="destructive optical burn validation",
    )
    if not iso_path.is_file():
        pytest.skip(f"ARC_DISC_GATED_BURN_ISO_PATH must be an ISO file: {iso_path}")
    if not os.access(device, os.R_OK | os.W_OK):
        pytest.skip(f"ARC_DISC_GATED_BURN_DEVICE must be readable and writable: {device}")
    copy_id = os.environ.get("ARC_DISC_GATED_BURN_COPY_ID", "gated-arc-disc-copy")

    burner = arc_disc_main.build_disc_burner()
    if not isinstance(burner, arc_disc_main.XorrisoDiscBurner):
        raise AssertionError(f"default burner is not the real backend: {type(burner).__name__}")
    verifier = arc_disc_main.build_burned_media_verifier()
    if not isinstance(verifier, arc_disc_main.RawBurnedMediaVerifier):
        raise AssertionError(
            f"default burned-media verifier is not the real backend: {type(verifier).__name__}"
        )

    burner.burn(iso_path, device=str(device), copy_id=copy_id)
    verifier.verify(iso_path, device=str(device), copy_id=copy_id)
