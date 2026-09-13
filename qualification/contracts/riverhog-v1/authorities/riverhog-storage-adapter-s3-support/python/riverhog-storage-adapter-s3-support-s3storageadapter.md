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
| Field | Shape |
|---|---|
| <a id="s-48f91609c5"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-5e5dd8006f"></a>`distribution` | "riverhog-storage-adapter-s3-support" |
| <a id="s-a47476af2f"></a>`module` | "riverhog_storage_adapter_s3_support" |
| <a id="s-6e09d2c4b3"></a>`name` | "S3StorageAdapter" |
| <a id="s-d367ca6c1a"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_s3_support.S3StorageAdapter.delete_prefix](riverhog-storage-adapter-s3-support-s3storageadapter-delete-prefix.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.complete_write](riverhog-storage-adapter-s3-support-s3storageadapter-complete-write.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.begin_write](riverhog-storage-adapter-s3-support-s3storageadapter-begin-write.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.write_segment](riverhog-storage-adapter-s3-support-s3storageadapter-write-segment.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.read_status](riverhog-storage-adapter-s3-support-s3storageadapter-read-status.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.prepare_read](riverhog-storage-adapter-s3-support-s3storageadapter-prepare-read.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.head_object](riverhog-storage-adapter-s3-support-s3storageadapter-head-object.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.list_segments](riverhog-storage-adapter-s3-support-s3storageadapter-list-segments.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.delete_object](riverhog-storage-adapter-s3-support-s3storageadapter-delete-object.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.cleanup_read](riverhog-storage-adapter-s3-support-s3storageadapter-cleanup-read.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.read_object](riverhog-storage-adapter-s3-support-s3storageadapter-read-object.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.abort_write](riverhog-storage-adapter-s3-support-s3storageadapter-abort-write.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.descriptor](riverhog-storage-adapter-s3-support-s3storageadapter-descriptor.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.find_completed_write](riverhog-storage-adapter-s3-support-s3storageadapter-find-completed-write.md)
- [riverhog_storage_adapter_s3_support.S3StorageAdapter.put_small_object](riverhog-storage-adapter-s3-support-s3storageadapter-put-small-object.md)

## Governing policies

- <a id="pa-e59b061af4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — `reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3StorageAdapter`

### Exact owned JSON

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
