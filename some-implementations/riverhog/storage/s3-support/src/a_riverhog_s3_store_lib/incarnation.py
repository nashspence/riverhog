"""Read durable storage identity outside an S3 adapter's object namespace."""

from __future__ import annotations

import hashlib
from contextlib import closing
from typing import Any
from uuid import uuid4

from botocore.exceptions import ClientError
from riverhog_storage_adapter_protocol import (
    StorageAdapterRejection,
    validate_storage_incarnation_id,
)

STORAGE_INCARNATION_MARKER_PREFIX = ".riverhog-storage-incarnation/"
_MAGIC = b"riverhog-storage-incarnation/v1\n"


class StorageIncarnationError(StorageAdapterRejection):
    """The configured S3 authority has no valid incarnation witness."""

    def __init__(self, message: str) -> None:
        super().__init__("provider_unavailable", message)


def marker_key(root_prefix: str) -> str:
    suffix = hashlib.sha256(root_prefix.encode("utf-8")).hexdigest()
    return f"{STORAGE_INCARNATION_MARKER_PREFIX}{suffix}"


def marker_document(incarnation_id: str) -> bytes:
    value = validate_storage_incarnation_id(incarnation_id)
    return _MAGIC + value.encode("ascii") + b"\n"


def read_storage_incarnation(client: Any, *, bucket: str, root_prefix: str) -> str:
    try:
        response = client.get_object(Bucket=bucket, Key=marker_key(root_prefix))
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404:
            raise StorageIncarnationError("S3 storage has no incarnation marker") from exc
        raise
    body = response.get("Body")
    if body is None:
        raise StorageIncarnationError("S3 incarnation response has no body")
    with closing(body):
        raw = body.read(128)
    if not isinstance(raw, bytes) or len(raw) != len(_MAGIC) + 37:
        raise StorageIncarnationError("S3 incarnation marker is invalid")
    if not raw.startswith(_MAGIC) or raw[-1:] != b"\n":
        raise StorageIncarnationError("S3 incarnation marker is invalid")
    try:
        return validate_storage_incarnation_id(raw[len(_MAGIC) : -1].decode("ascii"))
    except (UnicodeError, ValueError) as exc:
        raise StorageIncarnationError("S3 incarnation marker is invalid") from exc


def provision_storage_incarnation(client: Any, *, bucket: str, root_prefix: str) -> str:
    """Explicitly claim an empty root with one create-only marker write."""

    try:
        return read_storage_incarnation(client, bucket=bucket, root_prefix=root_prefix)
    except StorageIncarnationError as exc:
        if str(exc) != "S3 storage has no incarnation marker":
            raise
    object_prefix = f"{root_prefix}/" if root_prefix else ""
    listing = client.list_objects_v2(Bucket=bucket, Prefix=object_prefix, MaxKeys=1)
    if listing.get("KeyCount") or listing.get("Contents"):
        raise StorageIncarnationError("S3 storage root is not empty")
    proposed = str(uuid4())
    try:
        client.put_object(
            Bucket=bucket,
            Key=marker_key(root_prefix),
            Body=marker_document(proposed),
            IfNoneMatch="*",
        )
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status not in {409, 412}:
            raise
    observed = read_storage_incarnation(client, bucket=bucket, root_prefix=root_prefix)
    if observed != proposed:
        raise StorageIncarnationError("S3 root was claimed by another provisioner")
    return observed
