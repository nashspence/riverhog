# riverhog_storage_adapter_filesystem.FilesystemStorageAdapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-files-8c768c5c07:2a29e4a561 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2868267751"></a>
| Field | Shape |
|---|---|
| <a id="s-2744c5586c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-6455d35423"></a>`distribution` | "riverhog-storage-adapter-filesystem" |
| <a id="s-51848400d3"></a>`module` | "riverhog_storage_adapter_filesystem" |
| <a id="s-b1c5bfecb8"></a>`name` | "FilesystemStorageAdapter" |
| <a id="s-f7e8981ed7"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.put_small_object](riverhog-storage-adapter-filesystem-filesystemstorageadapter-put-small-object.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.close](riverhog-storage-adapter-filesystem-filesystemstorageadapter-close.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.read_object](riverhog-storage-adapter-filesystem-filesystemstorageadapter-read-object.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.__enter__](riverhog-storage-adapter-filesystem-filesystemstorageadapter-enter.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.delete_prefix](riverhog-storage-adapter-filesystem-filesystemstorageadapter-delete-prefix.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.head_object](riverhog-storage-adapter-filesystem-filesystemstorageadapter-head-object.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.list_segments](riverhog-storage-adapter-filesystem-filesystemstorageadapter-list-segments.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.readiness](riverhog-storage-adapter-filesystem-filesystemstorageadapter-readiness.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.delete_object](riverhog-storage-adapter-filesystem-filesystemstorageadapter-delete-object.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.write_segment](riverhog-storage-adapter-filesystem-filesystemstorageadapter-write-segment.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.prepare_read](riverhog-storage-adapter-filesystem-filesystemstorageadapter-prepare-read.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.cleanup_read](riverhog-storage-adapter-filesystem-filesystemstorageadapter-cleanup-read.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.begin_write](riverhog-storage-adapter-filesystem-filesystemstorageadapter-begin-write.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.__exit__](riverhog-storage-adapter-filesystem-filesystemstorageadapter-exit.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.abort_write](riverhog-storage-adapter-filesystem-filesystemstorageadapter-abort-write.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.read_status](riverhog-storage-adapter-filesystem-filesystemstorageadapter-read-status.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.complete_write](riverhog-storage-adapter-filesystem-filesystemstorageadapter-complete-write.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.find_completed_write](riverhog-storage-adapter-filesystem-filesystemstorageadapter-find-completed-write.md)
- [riverhog_storage_adapter_filesystem.FilesystemStorageAdapter.descriptor](riverhog-storage-adapter-filesystem-filesystemstorageadapter-descriptor.md)

## Governing policies

- <a id="pa-3fe5323ed3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../../../evidence/sources.md#src-e075952170) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_filesystem.FilesystemStorageAdapter`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 120cec8adf3f4622ed2757a91626ff47fb0b227977d2fde7419d58e6873b4010 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(config: 'FilesystemStorageAdapterConfig') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-filesystem",
  "module": "riverhog_storage_adapter_filesystem",
  "name": "FilesystemStorageAdapter",
  "unit": "export"
}
```
