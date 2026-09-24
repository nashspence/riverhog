# a_riverhog_s3_store_lib.S3StorageAdapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3storageadapter:10ea6f82bc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8d907691f0"></a>
- <a id="s-db21976023"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-9bafb294b1"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-a6125cecd9"></a>`name`: `S3StorageAdapter`
- <a id="s-a677263dd0"></a>`unit`: `export`

### Declared structure

- <a id="s-cdc84e09f7"></a>`kind`: `"class"`
- <a id="s-041561ff21"></a>`signature`: `"\"(client: 'Any', config: 'S3StorageAdapterConfig', *, read_preparation: 'S3ReadPreparation \| None' = None, object_reader: 'S3ObjectReader \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [put_small_object](a-riverhog-s3-store-lib-s3storageadapter-put-small-object.md)
- [delete_object](a-riverhog-s3-store-lib-s3storageadapter-delete-object.md)
- [write_segment](a-riverhog-s3-store-lib-s3storageadapter-write-segment.md)
- [list_segments](a-riverhog-s3-store-lib-s3storageadapter-list-segments.md)
- [abort_write](a-riverhog-s3-store-lib-s3storageadapter-abort-write.md)
- [delete_prefix](a-riverhog-s3-store-lib-s3storageadapter-delete-prefix.md)
- [begin_write](a-riverhog-s3-store-lib-s3storageadapter-begin-write.md)
- [cleanup_read](a-riverhog-s3-store-lib-s3storageadapter-cleanup-read.md)
- [complete_write](a-riverhog-s3-store-lib-s3storageadapter-complete-write.md)
- [find_completed_write](a-riverhog-s3-store-lib-s3storageadapter-find-completed-write.md)
- [prepare_read](a-riverhog-s3-store-lib-s3storageadapter-prepare-read.md)
- [descriptor](a-riverhog-s3-store-lib-s3storageadapter-descriptor.md)
- [head_object](a-riverhog-s3-store-lib-s3storageadapter-head-object.md)
- [read_object](a-riverhog-s3-store-lib-s3storageadapter-read-object.md)
- [read_status](a-riverhog-s3-store-lib-s3storageadapter-read-status.md)

## Governing policies

- <a id="pa-0afd213fdc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3StorageAdapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1a2e3ab7541f98c0c13a6cf439b319ead0baecc723d56e652601688e4a4bf59 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(client: 'Any', config: 'S3StorageAdapterConfig', *, read_preparation: 'S3ReadPreparation | None' = None, object_reader: 'S3ObjectReader | None' = None) -> 'None'\""
  },
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "S3StorageAdapter",
  "unit": "export"
}
```

</details>
