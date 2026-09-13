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
| Field | Shape |
|---|---|
| <a id="s-c77166329b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-16fc6fa4ec"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-7736db05f1"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-4b2111eef2"></a>`name` | "StorageAdapterPort" |
| <a id="s-8c630f98b1"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.StorageAdapterPort.begin_write](riverhog-storage-adapter-protocol-storageadapterport-begin-write.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.read_status](riverhog-storage-adapter-protocol-storageadapterport-read-status.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.delete_prefix](riverhog-storage-adapter-protocol-storageadapterport-delete-prefix.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.find_completed_write](riverhog-storage-adapter-protocol-storageadapterport-find-completed-write.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.read_object](riverhog-storage-adapter-protocol-storageadapterport-read-object.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.write_segment](riverhog-storage-adapter-protocol-storageadapterport-write-segment.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.delete_object](riverhog-storage-adapter-protocol-storageadapterport-delete-object.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.descriptor](riverhog-storage-adapter-protocol-storageadapterport-descriptor.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.abort_write](riverhog-storage-adapter-protocol-storageadapterport-abort-write.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.list_segments](riverhog-storage-adapter-protocol-storageadapterport-list-segments.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.prepare_read](riverhog-storage-adapter-protocol-storageadapterport-prepare-read.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.complete_write](riverhog-storage-adapter-protocol-storageadapterport-complete-write.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.put_small_object](riverhog-storage-adapter-protocol-storageadapterport-put-small-object.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.cleanup_read](riverhog-storage-adapter-protocol-storageadapterport-cleanup-read.md)
- [riverhog_storage_adapter_protocol.StorageAdapterPort.head_object](riverhog-storage-adapter-protocol-storageadapterport-head-object.md)

## Governing policies

- <a id="pa-7ae335c389"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterPort`

### Exact owned JSON

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
