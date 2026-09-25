from __future__ import annotations

from io import BytesIO

import pytest
from a_riverhog_s3_store_lib.incarnation import (
    StorageIncarnationError,
    marker_key,
    provision_storage_incarnation,
    read_storage_incarnation,
)
from botocore.exceptions import ClientError


class _Bucket:
    def __init__(self, *, occupied: bool = False) -> None:
        self.marker: bytes | None = None
        self.occupied = occupied

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        assert Bucket == "fixture" and Key == marker_key("")
        if self.marker is None:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                "GetObject",
            )
        return {"Body": BytesIO(self.marker)}

    def list_objects_v2(self, **request: object) -> dict[str, object]:
        assert request == {"Bucket": "fixture", "Prefix": "", "MaxKeys": 1}
        return {"KeyCount": 1 if self.occupied else 0}

    def put_object(self, **request: object) -> None:
        assert request.keys() == {"Bucket", "Key", "Body", "IfNoneMatch"}
        assert request["Bucket"] == "fixture"
        assert request["Key"] == marker_key("")
        assert request["IfNoneMatch"] == "*"
        assert self.marker is None
        self.marker = request["Body"]  # type: ignore[assignment]


def test_provisioning_is_explicit_create_only_and_stable() -> None:
    bucket = _Bucket()
    incarnation_id = provision_storage_incarnation(bucket, bucket="fixture", root_prefix="")
    assert read_storage_incarnation(bucket, bucket="fixture", root_prefix="") == incarnation_id
    assert provision_storage_incarnation(bucket, bucket="fixture", root_prefix="") == incarnation_id


def test_provisioning_refuses_nonempty_root() -> None:
    bucket = _Bucket(occupied=True)
    with pytest.raises(StorageIncarnationError, match="not empty"):
        provision_storage_incarnation(bucket, bucket="fixture", root_prefix="")
    assert bucket.marker is None
