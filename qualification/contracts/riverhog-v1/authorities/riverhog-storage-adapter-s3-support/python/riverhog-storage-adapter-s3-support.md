# riverhog_storage_adapter_s3_support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support:3267d72cba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-27c0a1e6bb) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3dfb383c0c"></a>
| Field | Shape |
|---|---|
| <a id="s-304c1205e5"></a>`candidate_id` | "python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support" |
| <a id="s-1f38987cfd"></a>`distribution` | "riverhog-storage-adapter-s3-support" |
| <a id="s-6d6569eece"></a>`exports` | additional keys=`S3ClientConfig`, `S3ReadPreparation`, `S3StorageAdapter`, `S3StorageAdapterConfig`, `S3TransportTuning`, `create_s3_client` |
| <a id="s-2a95ef6254"></a>`module` | "riverhog_storage_adapter_s3_support" |

## Governing policies

- <a id="pa-4773cd601f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — `reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py`

### Machine authority

- `/external_contract/python/29`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 749a3fbc18d77a9ffb46f09c12f55bac2083b33f300191c34f021212461558cc -->

```json
{
  "candidate_id": "python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support",
  "distribution": "riverhog-storage-adapter-s3-support",
  "exports": {
    "S3ClientConfig": {
      "fields": [
        {
          "default": "required",
          "name": "endpoint_url",
          "type": "'str | None'"
        },
        {
          "default": "required",
          "name": "region",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "access_key_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "secret_access_key",
          "type": "'str'"
        },
        {
          "default": "None",
          "name": "session_token",
          "type": "'str | None'"
        },
        {
          "default": "False",
          "name": "force_path_style",
          "type": "'bool'"
        }
      ],
      "kind": "class",
      "signature": "\"(endpoint_url: 'str | None', region: 'str', access_key_id: 'str', secret_access_key: 'str', session_token: 'str | None' = None, force_path_style: 'bool' = False) -> None\""
    },
    "S3ReadPreparation": {
      "kind": "class",
      "members": {
        "cleanup": {
          "kind": "method",
          "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'None'\""
        },
        "prepare": {
          "kind": "method",
          "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'ReadReadiness'\""
        },
        "status": {
          "kind": "method",
          "signature": "\"(self, *, client: 'Any', bucket: 'str', objects: 'tuple[tuple[str, str | None], ...]') -> 'ReadReadiness'\""
        }
      },
      "signature": "'(*args, **kwargs)'"
    },
    "S3StorageAdapter": {
      "kind": "class",
      "members": {
        "abort_write": {
          "kind": "method",
          "signature": "\"(self, session: 'WriteSession') -> 'None'\""
        },
        "begin_write": {
          "kind": "method",
          "signature": "\"(self, request: 'WriteStartRequest') -> 'WriteSession'\""
        },
        "cleanup_read": {
          "kind": "method",
          "signature": "\"(self, request: 'ReadPreparationRequest') -> 'None'\""
        },
        "complete_write": {
          "kind": "method",
          "signature": "\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""
        },
        "delete_object": {
          "kind": "method",
          "signature": "\"(self, request: 'DeleteObjectRequest') -> 'None'\""
        },
        "delete_prefix": {
          "kind": "method",
          "signature": "\"(self, request: 'DeletePrefixRequest') -> 'int'\""
        },
        "descriptor": {
          "kind": "method",
          "signature": "\"(self) -> 'AdapterDescriptor'\""
        },
        "find_completed_write": {
          "kind": "method",
          "signature": "\"(self, request: 'CompletedWriteLookupRequest') -> 'CompletedObjectReceipt | None'\""
        },
        "head_object": {
          "kind": "method",
          "signature": "\"(self, request: 'ObjectHeadRequest') -> 'ObjectMetadataReceipt | None'\""
        },
        "list_segments": {
          "kind": "method",
          "signature": "\"(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'\""
        },
        "prepare_read": {
          "kind": "method",
          "signature": "\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""
        },
        "put_small_object": {
          "kind": "method",
          "signature": "\"(self, request: 'SmallObjectWriteRequest', content: 'BinaryContent') -> 'ImmutableObjectReceipt'\""
        },
        "read_object": {
          "kind": "method",
          "signature": "\"(self, request: 'ObjectReadRequest') -> 'ObjectReadStream'\""
        },
        "read_status": {
          "kind": "method",
          "signature": "\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""
        },
        "write_segment": {
          "kind": "method",
          "signature": "\"(self, *, session: 'WriteSession', number: 'int', stored_bytes: 'int', content: 'BinaryContent') -> 'WriteSegmentReceipt'\""
        }
      },
      "signature": "\"(client: 'Any', config: 'S3StorageAdapterConfig', *, read_preparation: 'S3ReadPreparation | None' = None, object_reader: 'S3ObjectReader | None' = None) -> 'None'\""
    },
    "S3StorageAdapterConfig": {
      "fields": [
        {
          "default": "required",
          "name": "implementation_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "implementation_version",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "bucket",
          "type": "'str'"
        },
        {
          "default": "''",
          "name": "root_prefix",
          "type": "'str'"
        },
        {
          "default": "'immediate'",
          "name": "read_mode",
          "type": "'ReadMode'"
        },
        {
          "default": "None",
          "name": "archive_storage_class",
          "type": "'str | None'"
        },
        {
          "default": "None",
          "name": "immediate_storage_class",
          "type": "'str | None'"
        },
        {
          "default": "8388608",
          "name": "read_chunk_bytes",
          "type": "'int'"
        }
      ],
      "kind": "class",
      "signature": "\"(implementation_id: 'str', implementation_version: 'str', bucket: 'str', root_prefix: 'str' = '', read_mode: 'ReadMode' = 'immediate', archive_storage_class: 'str | None' = None, immediate_storage_class: 'str | None' = None, read_chunk_bytes: 'int' = 8388608) -> None\""
    },
    "S3TransportTuning": {
      "fields": [
        {
          "default": "32",
          "name": "max_pool_connections",
          "type": "'int'"
        },
        {
          "default": "10.0",
          "name": "connect_timeout_seconds",
          "type": "'float'"
        },
        {
          "default": "300.0",
          "name": "read_timeout_seconds",
          "type": "'float'"
        },
        {
          "default": "8",
          "name": "max_attempts",
          "type": "'int'"
        },
        {
          "default": "'standard'",
          "name": "retry_mode",
          "type": "\"Literal['standard', 'adaptive']\""
        },
        {
          "default": "True",
          "name": "tcp_keepalive",
          "type": "'bool'"
        }
      ],
      "kind": "class",
      "signature": "'(max_pool_connections: \\'int\\' = 32, connect_timeout_seconds: \\'float\\' = 10.0, read_timeout_seconds: \\'float\\' = 300.0, max_attempts: \\'int\\' = 8, retry_mode: \"Literal[\\'standard\\', \\'adaptive\\']\" = \\'standard\\', tcp_keepalive: \\'bool\\' = True) -> None'"
    },
    "create_s3_client": {
      "kind": "function",
      "signature": "\"(config: 'S3ClientConfig', *, tuning: 'S3TransportTuning | None' = None) -> 'Any'\""
    }
  },
  "module": "riverhog_storage_adapter_s3_support"
}
```
