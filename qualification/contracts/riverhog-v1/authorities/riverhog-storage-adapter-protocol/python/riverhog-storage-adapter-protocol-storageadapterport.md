# riverhog_storage_adapter_protocol.StorageAdapterPort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storageadapterport:b980780f97 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-209289da01"></a>
- <a id="s-16fc6fa4ec"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-7736db05f1"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-4b2111eef2"></a>`name`: `StorageAdapterPort`
- <a id="s-8c630f98b1"></a>`unit`: `export`

### Declared structure

- <a id="s-1224381e46"></a>`kind`: `"class"`
- <a id="s-b5162d97dd"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [begin_write](riverhog-storage-adapter-protocol-storageadapterport-begin-write.md)
- [read_status](riverhog-storage-adapter-protocol-storageadapterport-read-status.md)
- [delete_prefix](riverhog-storage-adapter-protocol-storageadapterport-delete-prefix.md)
- [find_completed_write](riverhog-storage-adapter-protocol-storageadapterport-find-completed-write.md)
- [read_object](riverhog-storage-adapter-protocol-storageadapterport-read-object.md)
- [write_segment](riverhog-storage-adapter-protocol-storageadapterport-write-segment.md)
- [delete_object](riverhog-storage-adapter-protocol-storageadapterport-delete-object.md)
- [descriptor](riverhog-storage-adapter-protocol-storageadapterport-descriptor.md)
- [abort_write](riverhog-storage-adapter-protocol-storageadapterport-abort-write.md)
- [list_segments](riverhog-storage-adapter-protocol-storageadapterport-list-segments.md)
- [prepare_read](riverhog-storage-adapter-protocol-storageadapterport-prepare-read.md)
- [complete_write](riverhog-storage-adapter-protocol-storageadapterport-complete-write.md)
- [put_small_object](riverhog-storage-adapter-protocol-storageadapterport-put-small-object.md)
- [cleanup_read](riverhog-storage-adapter-protocol-storageadapterport-cleanup-read.md)
- [head_object](riverhog-storage-adapter-protocol-storageadapterport-head-object.md)

## Governing policies

- <a id="pa-7ae335c389"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterPort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c8bbf2058db5f78787a4c1613fb7091b6826a31b51bf3948ead50e53d02118aa -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "StorageAdapterPort",
  "unit": "export"
}
```

</details>
