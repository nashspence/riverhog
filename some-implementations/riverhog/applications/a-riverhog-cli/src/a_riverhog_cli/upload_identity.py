"""Exact application-owned source identity in an explicitly selected byte store."""

from __future__ import annotations

import json
from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import closing, contextmanager
from threading import RLock
from typing import BinaryIO

from riverhog_canonical_json import canonical_json_bytes
from riverhog_client import PrimaryByteFile, PrimaryStateCapacity, PrimaryStateError, PrimaryStorage

_REFERENCE = "upload-identity"
_CONTENT = "source-identity"
_STAGED_CONTENT = "staged-source-identity"
_WINDOW = 4096
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


@contextmanager
def open_upload_identity(storage: PrimaryStorage, *, byte_limit: int) -> Iterator[BinaryIO]:
    """Borrow a verified original reader under the selected storage owner.

    Verification completes before the reader is exposed. The caller owns its
    decoding buffers and must consume the reader within this context. Absence
    never authorizes initialization, and reader close performs no reclamation.
    """
    _admit(byte_limit)
    reference = _reference(storage.read_control(_REFERENCE, byte_limit=_REFERENCE_BYTES))
    if reference.bytes > byte_limit:
        raise PrimaryStateCapacity("retained upload identity exceeds the byte admission")
    with storage.verified(reference) as stream:
        yield stream


def _prepared_chunks(
    chunks: Iterable[bytes], *, byte_limit: int, prepare_payload: Callable[[], None]
) -> Generator[bytes, None, None]:
    pending = bytearray()
    seen = 0
    for chunk in chunks:
        if type(chunk) is not bytes:
            raise TypeError("upload identity chunks require immutable bytes")
        if len(chunk) > byte_limit - seen:
            raise PrimaryStateCapacity("upload identity exceeds the byte admission")
        seen += len(chunk)
        offset = 0
        while offset < len(chunk):
            take = min(_WINDOW - len(pending), len(chunk) - offset)
            pending.extend(memoryview(chunk)[offset : offset + take])
            offset += take
            if len(pending) == _WINDOW:
                prepare_payload()
                yield bytes(pending)
                pending.clear()
    if pending:
        prepare_payload()
        yield bytes(pending)


def stage_upload_identity(
    storage: PrimaryStorage,
    chunks: Iterable[bytes],
    *,
    byte_limit: int,
    prepare_payload: Callable[[], None],
) -> PrimaryByteFile:
    """Retain exact original encoding in bounded windows under the same owner.

    The staging extent is nonrebuildable input, retained for the allocation's
    lifetime within its original physical grants. No reference is selected
    before the complete staged commitment exists. The backend retains its
    construction obligations on failure. Missing final identity on resume
    never authorizes reconstructing it from staging or allocating new IDs.
    """
    _admit(byte_limit)
    if not callable(prepare_payload):
        raise TypeError("upload identity requires explicit payload preparation")
    with _initialization_lock:
        if any(storage.exists(name) for name in (_REFERENCE, _CONTENT, _STAGED_CONTENT)):
            raise FileExistsError("fresh upload initialization conflicts with retained identity")
        source = iter(chunks)
        try:
            prepare_payload()
            with closing(
                _prepared_chunks(source, byte_limit=byte_limit, prepare_payload=prepare_payload)
            ) as prepared:
                selected = storage.put_bytes(_STAGED_CONTENT, prepared, byte_limit=byte_limit)
            storage.sync()
            return selected
        finally:
            close = getattr(source, "close", None)
            if callable(close):
                close()


@contextmanager
def initialize_upload_identity(
    storage: PrimaryStorage,
    candidate: PrimaryByteFile,
    *,
    byte_limit: int,
    control_capacity: int,
    prepare_payload: Callable[[], None],
) -> Iterator[BinaryIO]:
    """Select original authority before final payload copying, then borrow it.

    Fresh initialization is serialized within this application. Selected
    references and content are never replaced. The complete staging extent is
    verified before selecting its final reference. That reference commits before
    copying the final payload, so interruption cannot authorize a new hash or IDs.
    Missing/incomplete content on resume remains an error. The caller supplies
    resource preparation before each bounded copy window. The staging extent
    remains retained; reader close performs no reclamation or preparation.
    The selected control capacity and the logical reference bound are independent.
    Initialization ownership lasts through the borrowed original reader.
    """
    _admit(byte_limit)
    _admit(control_capacity)
    if type(candidate) is not PrimaryByteFile:
        raise TypeError("upload identity requires an immutable staged commitment")
    if candidate.name in (_REFERENCE, _CONTENT):
        raise ValueError("upload identity staging requires a separate owned extent")
    if candidate.bytes > byte_limit:
        raise PrimaryStateCapacity("upload identity exceeds the byte admission")
    if not callable(prepare_payload):
        raise TypeError("upload identity requires explicit payload preparation")
    with _initialization_lock:
        if storage.exists(_REFERENCE) or storage.exists(_CONTENT):
            raise FileExistsError("fresh upload initialization conflicts with retained identity")
        reference = PrimaryByteFile(name=_CONTENT, bytes=candidate.bytes, sha256=candidate.sha256)
        encoded = canonical_json_bytes(
            {"format": _FORMAT, "bytes": str(reference.bytes), "sha256": reference.sha256}
        )
        if len(encoded) > min(_REFERENCE_BYTES, control_capacity):
            raise PrimaryStateCapacity("upload identity reference exceeds the control admission")
        with storage.verified(candidate) as original:
            storage.prepare_controls((_REFERENCE,), byte_limit=control_capacity)
            storage.put_control(_REFERENCE, encoded, byte_limit=_REFERENCE_BYTES)
            storage.sync()
            prepare_payload()

            def chunks() -> Generator[bytes, None, None]:
                while body := original.read(_WINDOW):
                    prepare_payload()
                    yield body

            with closing(chunks()) as copied:
                selected = storage.put_bytes(
                    _CONTENT,
                    copied,
                    byte_limit=byte_limit,
                    expected=(reference.bytes, reference.sha256),
                )
            if selected != reference:
                raise PrimaryStateError("upload identity selected a different original")
        storage.sync()
        with open_upload_identity(storage, byte_limit=byte_limit) as stream:
            yield stream
