# riverhog_storage_adapter_s3_support.S3StorageAdapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3storageadapter:fa9a6d8847 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6871f3d17b"></a>
- <a id="s-5e5dd8006f"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-a47476af2f"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-6e09d2c4b3"></a>`name`: `S3StorageAdapter`
- <a id="s-d367ca6c1a"></a>`unit`: `export`

### Declared structure

- <a id="s-e33effa157"></a>`kind`: `"class"`
- <a id="s-6eb96acebd"></a>`signature`: `"\"(client: 'Any', config: 'S3StorageAdapterConfig', *, read_preparation: 'S3ReadPreparation \| None' = None, object_reader: 'S3ObjectReader \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [delete_prefix](riverhog-storage-adapter-s3-support-s3storageadapter-delete-prefix.md)
- [complete_write](riverhog-storage-adapter-s3-support-s3storageadapter-complete-write.md)
- [begin_write](riverhog-storage-adapter-s3-support-s3storageadapter-begin-write.md)
- [write_segment](riverhog-storage-adapter-s3-support-s3storageadapter-write-segment.md)
- [read_status](riverhog-storage-adapter-s3-support-s3storageadapter-read-status.md)
- [prepare_read](riverhog-storage-adapter-s3-support-s3storageadapter-prepare-read.md)
- [head_object](riverhog-storage-adapter-s3-support-s3storageadapter-head-object.md)
- [list_segments](riverhog-storage-adapter-s3-support-s3storageadapter-list-segments.md)
- [delete_object](riverhog-storage-adapter-s3-support-s3storageadapter-delete-object.md)
- [cleanup_read](riverhog-storage-adapter-s3-support-s3storageadapter-cleanup-read.md)
- [read_object](riverhog-storage-adapter-s3-support-s3storageadapter-read-object.md)
- [abort_write](riverhog-storage-adapter-s3-support-s3storageadapter-abort-write.md)
- [descriptor](riverhog-storage-adapter-s3-support-s3storageadapter-descriptor.md)
- [find_completed_write](riverhog-storage-adapter-s3-support-s3storageadapter-find-completed-write.md)
- [put_small_object](riverhog-storage-adapter-s3-support-s3storageadapter-put-small-object.md)

## Governing policies

- <a id="pa-e59b061af4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — [reference/riverhog/storage/s3-support/src/riverhog\_storage\_adapter\_s3\_support/\_\_init\_\_.py](../../../../../../reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3StorageAdapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ee41621dab8f9a2ad76dc6f80962337c4f3d4308f1d6db2c54b309c5c8be84d -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(client: 'Any', config: 'S3StorageAdapterConfig', *, read_preparation: 'S3ReadPreparation | None' = None, object_reader: 'S3ObjectReader | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "S3StorageAdapter",
  "unit": "export"
}
```

</details>
