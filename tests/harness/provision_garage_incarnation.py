"""Explicit test-only provisioning for a Garage adapter bucket."""

from __future__ import annotations

import os
from uuid import uuid4

from a_riverhog_s3_store_lib import S3ClientConfig, create_s3_client
from a_riverhog_s3_store_lib.incarnation import (
    StorageIncarnationError,
    marker_document,
    marker_key,
    read_storage_incarnation,
)


def main() -> None:
    bucket = os.environ["RIVERHOG_GARAGE_PROVISION_BUCKET"]
    client = create_s3_client(
        S3ClientConfig(
            endpoint_url=os.environ.get("RIVERHOG_GARAGE_PROVISION_ENDPOINT", "http://garage:3900"),
            region=os.environ.get("RIVERHOG_GARAGE_PROVISION_REGION", "garage"),
            access_key_id=os.environ["RIVERHOG_GARAGE_PROVISION_ACCESS_KEY_ID"],
            secret_access_key=os.environ["RIVERHOG_GARAGE_PROVISION_SECRET_ACCESS_KEY"],
            force_path_style=True,
        )
    )
    try:
        read_storage_incarnation(client, bucket=bucket, root_prefix="")
    except StorageIncarnationError as exc:
        if str(exc) != "S3 storage has no incarnation marker":
            raise
        client.put_object(
            Bucket=bucket,
            Key=marker_key(""),
            Body=marker_document(str(uuid4())),
        )
        read_storage_incarnation(client, bucket=bucket, root_prefix="")


if __name__ == "__main__":
    main()
