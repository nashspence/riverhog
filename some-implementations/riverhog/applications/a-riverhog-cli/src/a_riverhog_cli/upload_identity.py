"""Exact application-owned source identity in an explicitly selected byte store."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from threading import RLock

from riverhog_canonical_json import canonical_json_bytes
from riverhog_client import PrimaryByteFile, PrimaryStateCapacity, PrimaryStateError, PrimaryStorage

_REFERENCE = "upload-identity"
_CONTENT = "source-identity"
_FORMAT = "a-riverhog-cli-source-identity/v1"
_REFERENCE_BYTES = 1024
_initialization_lock = RLock()


def _admit(byte_limit: int) -> None:
    if type(byte_limit) is not int or byte_limit < 1:
        raise ValueError("upload identity requires a positive byte admission")


def _reference(body: bytes) -> PrimaryByteFile:
    try:
        document = json.loads(body)
        if (
            not isinstance(document, dict)
            or set(document) != {"format", "bytes", "sha256"}
            or document["format"] != _FORMAT
        ):
            raise ValueError
        count = document["bytes"]
        if (
            not isinstance(count, str)
            or not count.isascii()
            or not count.isdecimal()
            or (count != "0" and count.startswith("0"))
        ):
            raise ValueError
        return PrimaryByteFile(name=_CONTENT, bytes=int(count), sha256=document["sha256"])
    except (TypeError, ValueError, KeyError) as error:
        raise PrimaryStateError("retained upload identity reference is invalid") from error


def read_upload_identity(storage: PrimaryStorage, *, byte_limit: int) -> bytes:
    """Verify the recorded original bytes; absence never authorizes initialization.

    The byte admission covers this stored identity and its replay buffer. The
    caller separately owns source enumeration and identity serialization buffers.
    """
    _admit(byte_limit)
    reference = _reference(storage.read_control(_REFERENCE, byte_limit=_REFERENCE_BYTES))
    if reference.bytes > byte_limit:
        raise PrimaryStateCapacity("retained upload identity exceeds the byte admission")
    with storage.verified(reference) as stream:
        body = stream.read(reference.bytes + 1)
    if len(body) != reference.bytes:
        raise PrimaryStateError("retained upload identity length differs")
    return body


def initialize_upload_identity(
    storage: PrimaryStorage,
    body: bytes,
    *,
    byte_limit: int,
    prepare_payload: Callable[[], None],
) -> bytes:
    """Record original authority, prepare resources, then select the exact bytes.

    Fresh initialization is serialized within this application. Selected
    references and content are never replaced. The original reference commits
    before payload work, so interruption cannot authorize a new hash or IDs.
    Missing/incomplete content on resume remains an error. The calling owner
    supplies actual resource preparation before immutable byte publication.
    This function neither erases failed state nor drives initialization on read.
    """
    _admit(byte_limit)
    if type(body) is not bytes:
        raise TypeError("upload identity requires immutable bytes")
    if len(body) > byte_limit:
        raise PrimaryStateCapacity("upload identity exceeds the byte admission")
    if not callable(prepare_payload):
        raise TypeError("upload identity requires explicit payload preparation")
    with _initialization_lock:
        if storage.exists(_REFERENCE) or storage.exists(_CONTENT):
            raise FileExistsError("fresh upload initialization conflicts with retained identity")
        reference = PrimaryByteFile(
            name=_CONTENT, bytes=len(body), sha256=hashlib.sha256(body).hexdigest()
        )
        encoded = canonical_json_bytes(
            {"format": _FORMAT, "bytes": str(reference.bytes), "sha256": reference.sha256}
        )
        storage.prepare_controls((_REFERENCE,), byte_limit=_REFERENCE_BYTES)
        storage.put_control(_REFERENCE, encoded, byte_limit=_REFERENCE_BYTES)
        storage.sync()
        prepare_payload()
        selected = storage.put_bytes(
            _CONTENT,
            (body,),
            byte_limit=byte_limit,
            expected=(reference.bytes, reference.sha256),
        )
        if selected != reference:
            raise PrimaryStateError("upload identity selected a different original")
        storage.sync()
        return read_upload_identity(storage, byte_limit=byte_limit)
