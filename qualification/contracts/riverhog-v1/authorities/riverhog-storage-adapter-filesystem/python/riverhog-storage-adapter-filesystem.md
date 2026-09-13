# riverhog_storage_adapter_filesystem

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem:d7e29ca4e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-3893aeaa1c) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8b784b840a"></a>
| Field | Shape |
|---|---|
| <a id="s-a54044dfea"></a>`candidate_id` | "python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem" |
| <a id="s-d04b502d8f"></a>`distribution` | "riverhog-storage-adapter-filesystem" |
| <a id="s-7d6c0ef79c"></a>`exports` | additional keys=`FilesystemStorageAdapter`, `FilesystemStorageAdapterConfig` |
| <a id="s-bb1aabcd9d"></a>`module` | "riverhog_storage_adapter_filesystem" |

## Governing policies

- <a id="pa-e98d637915"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources.md#src-e075952170) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py`

### Machine authority

- `/external_contract/python/27`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21515a2b92a6d70f9b70dc057df7cc30c4e77152148143afd5eabefde033c137 -->

```json
{
  "candidate_id": "python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem",
  "distribution": "riverhog-storage-adapter-filesystem",
  "exports": {
    "FilesystemStorageAdapter": {
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
        "close": {
          "kind": "method",
          "signature": "\"(self) -> 'None'\""
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
        "readiness": {
          "kind": "method",
          "signature": "\"(self) -> 'None'\""
        },
        "write_segment": {
          "kind": "method",
          "signature": "\"(self, *, session: 'WriteSession', number: 'int', stored_bytes: 'int', content: 'BinaryContent') -> 'WriteSegmentReceipt'\""
        }
      },
      "signature": "\"(config: 'FilesystemStorageAdapterConfig') -> 'None'\""
    },
    "FilesystemStorageAdapterConfig": {
      "fields": [
        {
          "default": "required",
          "name": "root",
          "type": "'Path'"
        },
        {
          "default": "'riverhog.filesystem/v1'",
          "name": "implementation_id",
          "type": "'str'"
        },
        {
          "default": "'0.1.0'",
          "name": "implementation_version",
          "type": "'str'"
        },
        {
          "default": "67108864",
          "name": "segment_bytes",
          "type": "'int'"
        },
        {
          "default": "8388608",
          "name": "read_chunk_bytes",
          "type": "'int'"
        },
        {
          "default": "268435456",
          "name": "minimum_free_bytes",
          "type": "'int'"
        }
      ],
      "kind": "class",
      "signature": "\"(root: 'Path', implementation_id: 'str' = 'riverhog.filesystem/v1', implementation_version: 'str' = '0.1.0', segment_bytes: 'int' = 67108864, read_chunk_bytes: 'int' = 8388608, minimum_free_bytes: 'int' = 268435456) -> None\""
    }
  },
  "module": "riverhog_storage_adapter_filesystem"
}
```
