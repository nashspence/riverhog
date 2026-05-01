from __future__ import annotations

import tempfile
import uuid
from collections.abc import Iterable
from typing import cast

from arc_core.domain.errors import NotFound
from arc_core.runtime_config import RuntimeConfig
from arc_core.stores.s3_support import create_s3_client


class S3HotStore:
    def __init__(self, config: RuntimeConfig) -> None:
        self._bucket = config.s3_bucket
        self._client = create_s3_client(config)

    def _key(self, collection_id: str, path: str) -> str:
        return f"collections/{collection_id}/{path}"

    def put_collection_file(self, collection_id: str, path: str, content: bytes) -> None:
        self._client.put_object(
            Bucket=self._bucket,
            Key=self._key(collection_id, path),
            Body=content,
        )

    def put_collection_file_stream(
        self,
        collection_id: str,
        path: str,
        chunks: Iterable[bytes],
        *,
        content_length: int,
    ) -> None:
        final_key = self._key(collection_id, path)
        temp_key = f"tmp/collections/{collection_id}/{uuid.uuid4().hex}"
        with tempfile.TemporaryFile() as body:
            size = 0
            for chunk in chunks:
                size += len(chunk)
                body.write(chunk)
            if size != content_length:
                raise ValueError(f"collection file stream byte count mismatch: {path}")
            body.seek(0)
            try:
                self._client.put_object(
                    Bucket=self._bucket,
                    Key=temp_key,
                    Body=body,
                    ContentLength=content_length,
                )
                self._client.copy_object(
                    Bucket=self._bucket,
                    Key=final_key,
                    CopySource={"Bucket": self._bucket, "Key": temp_key},
                )
            finally:
                self._client.delete_object(Bucket=self._bucket, Key=temp_key)

    def get_collection_file(self, collection_id: str, path: str) -> bytes:
        try:
            response = self._client.get_object(
                Bucket=self._bucket,
                Key=self._key(collection_id, path),
            )
        except self._client.exceptions.ClientError as exc:
            if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 404:
                raise
            raise NotFound(f"file not found in hot store: {collection_id}/{path}") from exc
        return cast(bytes, response["Body"].read())

    def has_collection_file(self, collection_id: str, path: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._key(collection_id, path))
            return True
        except self._client.exceptions.ClientError as exc:
            if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                return False
            raise

    def delete_collection_file(self, collection_id: str, path: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=self._key(collection_id, path))

    def list_collection_files(self, collection_id: str) -> list[tuple[str, int]]:
        paginator = self._client.get_paginator("list_objects_v2")
        prefix = f"collections/{collection_id}/"
        results: list[tuple[str, int]] = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for entry in page.get("Contents", []):
                key = str(entry["Key"])
                if key.endswith(".info") or key.endswith(".part"):
                    continue
                results.append((key.removeprefix(prefix), int(entry.get("Size", 0))))
        return sorted(results)
