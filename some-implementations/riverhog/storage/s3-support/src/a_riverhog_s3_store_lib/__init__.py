"""Common implementation support for scoped S3-backed storage adapters."""

from a_riverhog_s3_store_lib.adapter import (
    S3ReadPreparation,
    S3StorageAdapter,
    S3StorageAdapterConfig,
)
from a_riverhog_s3_store_lib.client import (
    S3ClientConfig,
    S3TransportTuning,
    create_s3_client,
)

__all__ = [
    "S3ClientConfig",
    "S3ReadPreparation",
    "S3StorageAdapter",
    "S3StorageAdapterConfig",
    "S3TransportTuning",
    "create_s3_client",
]
