# a_riverhog_filesystem_store.FilesystemStorageAdapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-filesystem-store:a-riverhog-filesystem-store-filesystemstorageadapter:1a3e43d06a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-filesystem-store](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-26d69d2a78"></a>
- <a id="s-4062b52cad"></a>`distribution`: `a-riverhog-filesystem-store`
- <a id="s-738b0c7703"></a>`module`: `a_riverhog_filesystem_store`
- <a id="s-f1af4f18a3"></a>`name`: `FilesystemStorageAdapter`
- <a id="s-9e59484454"></a>`unit`: `export`

### Declared structure

- <a id="s-6dcb371861"></a>`kind`: `"class"`
- <a id="s-a3980f722a"></a>`signature`: `"\"(config: 'FilesystemStorageAdapterConfig') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [abort_write](a-riverhog-filesystem-store-filesystemstorageadapter-abort-write.md)
- [readiness](a-riverhog-filesystem-store-filesystemstorageadapter-readiness.md)
- [complete_write](a-riverhog-filesystem-store-filesystemstorageadapter-complete-write.md)
- [close](a-riverhog-filesystem-store-filesystemstorageadapter-close.md)
- [delete_prefix](a-riverhog-filesystem-store-filesystemstorageadapter-delete-prefix.md)
- [write_segment](a-riverhog-filesystem-store-filesystemstorageadapter-write-segment.md)
- [cleanup_read](a-riverhog-filesystem-store-filesystemstorageadapter-cleanup-read.md)
- [put_small_object](a-riverhog-filesystem-store-filesystemstorageadapter-put-small-object.md)
- [__enter__](a-riverhog-filesystem-store-filesystemstorageadapter-enter.md)
- [prepare_read](a-riverhog-filesystem-store-filesystemstorageadapter-prepare-read.md)
- [read_status](a-riverhog-filesystem-store-filesystemstorageadapter-read-status.md)
- [list_segments](a-riverhog-filesystem-store-filesystemstorageadapter-list-segments.md)
- [head_object](a-riverhog-filesystem-store-filesystemstorageadapter-head-object.md)
- [delete_object](a-riverhog-filesystem-store-filesystemstorageadapter-delete-object.md)
- [descriptor](a-riverhog-filesystem-store-filesystemstorageadapter-descriptor.md)
- [find_completed_write](a-riverhog-filesystem-store-filesystemstorageadapter-find-completed-write.md)
- [__exit__](a-riverhog-filesystem-store-filesystemstorageadapter-exit.md)
- [read_object](a-riverhog-filesystem-store-filesystemstorageadapter-read-object.md)
- [begin_write](a-riverhog-filesystem-store-filesystemstorageadapter-begin-write.md)

## Governing policies

- <a id="pa-885422b2c2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-filesystem-store:a_riverhog_filesystem_store](../../../evidence/sources/authorities.md#src-d49395fbda) — [some-implementations/riverhog/storage/filesystem/src/a\_riverhog\_filesystem\_store/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_filesystem_store.FilesystemStorageAdapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 474894831c7d03a1632718381480b5b156163ff64b8cccfe2300f6a40db73158 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(config: 'FilesystemStorageAdapterConfig') -> 'None'\""
  },
  "distribution": "a-riverhog-filesystem-store",
  "module": "a_riverhog_filesystem_store",
  "name": "FilesystemStorageAdapter",
  "unit": "export"
}
```

</details>
