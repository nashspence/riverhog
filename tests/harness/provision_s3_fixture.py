"""Provision and qualify the disposable S3 fixture through its native API."""

from __future__ import annotations

import argparse
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from uuid import uuid4

from a_riverhog_s3_store_lib import S3ClientConfig, create_s3_client
from a_riverhog_s3_store_lib.incarnation import (
    StorageIncarnationError,
    marker_document,
    marker_key,
    read_storage_incarnation,
)
from botocore.exceptions import BotoCoreError, ClientError


def _wait_for_fixture(client: Any) -> None:
    deadline = time.monotonic() + 60
    while True:
        try:
            client.list_buckets()
            return
        except (BotoCoreError, ClientError):
            if time.monotonic() >= deadline:
                raise RuntimeError("S3 fixture did not become ready") from None
            time.sleep(0.2)


def _conditional_conflict(error: ClientError) -> bool:
    return error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") in {409, 412}


def _read(client: Any, *, bucket: str, key: str) -> bytes:
    body = client.get_object(Bucket=bucket, Key=key)["Body"]
    try:
        return body.read()
    finally:
        body.close()


def _publish_pair(client: Any, *, bucket: str, key: str, condition: dict[str, str]) -> str:
    prepared: list[tuple[str, str, bytes]] = []
    accepted: list[bool] = []
    try:
        # A replacement must change the content identity. Reusing the first
        # race's bytes can legitimately retain its ETag and admit another CAS.
        generation = uuid4().hex.encode("ascii")
        for payload in (generation + b"/first candidate", generation + b"/second candidate"):
            upload_id = client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
            prepared.append((upload_id, "", payload))
            part = client.upload_part(
                Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=payload
            )
            prepared[-1] = (upload_id, part["ETag"], payload)
        ready = threading.Barrier(2)

        def complete(candidate: tuple[str, str, bytes]) -> bool:
            upload_id, part_etag, _ = candidate
            ready.wait(10)
            try:
                client.complete_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part_etag}]},
                    **condition,
                )
                return True
            except ClientError as error:
                if not _conditional_conflict(error):
                    raise
                return False

        with ThreadPoolExecutor(max_workers=2) as pool:
            accepted = list(pool.map(complete, prepared))
        winners = [
            candidate[2] for candidate, success in zip(prepared, accepted, strict=True) if success
        ]
        retained = len(winners) == 1 and _read(client, bucket=bucket, key=key) == winners[0]
        if not retained:
            raise RuntimeError(
                "S3 fixture did not preserve exactly one conditional publication "
                f"({next(iter(condition))}: accepted={accepted}, winner_retained={retained})"
            )
        return str(client.head_object(Bucket=bucket, Key=key)["ETag"])
    finally:
        for index, (upload_id, _, _) in enumerate(prepared):
            if index < len(accepted) and accepted[index]:
                continue
            try:
                client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            except ClientError as error:
                if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 404:
                    raise


def verify_conditional_writes(client: Any, *, bucket: str) -> None:
    key = f"fixture-qualification/{uuid4().hex}"
    try:
        etag = _publish_pair(client, bucket=bucket, key=key, condition={"IfNoneMatch": "*"})
        original = _read(client, bucket=bucket, key=key)
        try:
            client.put_object(Bucket=bucket, Key=key, Body=b"must not replace", IfNoneMatch="*")
        except ClientError as error:
            if not _conditional_conflict(error):
                raise
        else:
            raise RuntimeError("S3 fixture ignored create-only PutObject")
        if _read(client, bucket=bucket, key=key) != original:
            raise RuntimeError("S3 fixture changed the rejected publication")
        _publish_pair(client, bucket=bucket, key=key, condition={"IfMatch": etag})
    finally:
        client.delete_object(Bucket=bucket, Key=key)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-conditional-writes", action="store_true")
    args = parser.parse_args()
    bucket = os.environ["RIVERHOG_S3_FIXTURE_PROVISION_BUCKET"]
    client = create_s3_client(
        S3ClientConfig(
            endpoint_url=os.environ.get(
                "RIVERHOG_S3_FIXTURE_ENDPOINT_URL", "http://s3-fixture:9000"
            ),
            region=os.environ.get("RIVERHOG_S3_FIXTURE_REGION", "us-east-1"),
            access_key_id=os.environ["RIVERHOG_S3_FIXTURE_ACCESS_KEY_ID"],
            secret_access_key=os.environ["RIVERHOG_S3_FIXTURE_SECRET_ACCESS_KEY"],
            force_path_style=True,
        )
    )
    try:
        _wait_for_fixture(client)
        try:
            client.head_bucket(Bucket=bucket)
        except ClientError as error:
            if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 404:
                raise
            client.create_bucket(Bucket=bucket)
        try:
            read_storage_incarnation(client, bucket=bucket, root_prefix="")
        except StorageIncarnationError as error:
            if str(error) != "S3 storage has no incarnation marker":
                raise
            client.put_object(
                Bucket=bucket,
                Key=marker_key(""),
                Body=marker_document(str(uuid4())),
                IfNoneMatch="*",
            )
            read_storage_incarnation(client, bucket=bucket, root_prefix="")
        if args.verify_conditional_writes:
            verify_conditional_writes(client, bucket=bucket)
            print("S3 fixture conditional publication verified")
    finally:
        client.close()


if __name__ == "__main__":
    main()
