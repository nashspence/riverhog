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
- <a id="s-6455d35423"></a>`distribution`: `riverhog-storage-adapter-filesystem`
- <a id="s-51848400d3"></a>`module`: `riverhog_storage_adapter_filesystem`
- <a id="s-b1c5bfecb8"></a>`name`: `FilesystemStorageAdapter`
- <a id="s-f7e8981ed7"></a>`unit`: `export`

### Declared structure

- <a id="s-6b8d53fd21"></a>`kind`: `"class"`
- <a id="s-dc3b75f083"></a>`signature`: `"\"(config: 'FilesystemStorageAdapterConfig') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [put_small_object](riverhog-storage-adapter-filesystem-filesystemstorageadapter-put-small-object.md)
- [close](riverhog-storage-adapter-filesystem-filesystemstorageadapter-close.md)
- [read_object](riverhog-storage-adapter-filesystem-filesystemstorageadapter-read-object.md)
- [__enter__](riverhog-storage-adapter-filesystem-filesystemstorageadapter-enter.md)
- [delete_prefix](riverhog-storage-adapter-filesystem-filesystemstorageadapter-delete-prefix.md)
- [head_object](riverhog-storage-adapter-filesystem-filesystemstorageadapter-head-object.md)
- [list_segments](riverhog-storage-adapter-filesystem-filesystemstorageadapter-list-segments.md)
- [readiness](riverhog-storage-adapter-filesystem-filesystemstorageadapter-readiness.md)
- [delete_object](riverhog-storage-adapter-filesystem-filesystemstorageadapter-delete-object.md)
- [write_segment](riverhog-storage-adapter-filesystem-filesystemstorageadapter-write-segment.md)
- [prepare_read](riverhog-storage-adapter-filesystem-filesystemstorageadapter-prepare-read.md)
- [cleanup_read](riverhog-storage-adapter-filesystem-filesystemstorageadapter-cleanup-read.md)
- [begin_write](riverhog-storage-adapter-filesystem-filesystemstorageadapter-begin-write.md)
- [__exit__](riverhog-storage-adapter-filesystem-filesystemstorageadapter-exit.md)
- [abort_write](riverhog-storage-adapter-filesystem-filesystemstorageadapter-abort-write.md)
- [read_status](riverhog-storage-adapter-filesystem-filesystemstorageadapter-read-status.md)
- [complete_write](riverhog-storage-adapter-filesystem-filesystemstorageadapter-complete-write.md)
- [find_completed_write](riverhog-storage-adapter-filesystem-filesystemstorageadapter-find-completed-write.md)
- [descriptor](riverhog-storage-adapter-filesystem-filesystemstorageadapter-descriptor.md)

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
