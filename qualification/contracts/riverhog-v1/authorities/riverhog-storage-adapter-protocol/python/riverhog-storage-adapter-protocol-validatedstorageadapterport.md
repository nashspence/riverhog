# riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-9d3e33d8b2:9db350e50b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-35baea0818"></a>
| Field | Shape |
|---|---|
| <a id="s-dac45ed62d"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-cde8be359e"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-caa384c6ab"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-a9d777d4b8"></a>`name` | "ValidatedStorageAdapterPort" |
| <a id="s-b6edc77857"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.read_status](riverhog-storage-adapter-protocol-validatedstorageadapterport-read-status.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.put_small_object](riverhog-storage-adapter-protocol-validatedstorageadapterport-put-small-object.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.abort_write](riverhog-storage-adapter-protocol-validatedstorageadapterport-abort-write.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.prepare_read](riverhog-storage-adapter-protocol-validatedstorageadapterport-prepare-read.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.complete_write](riverhog-storage-adapter-protocol-validatedstorageadapterport-complete-write.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.head_object](riverhog-storage-adapter-protocol-validatedstorageadapterport-head-object.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.find_completed_write](riverhog-storage-adapter-protocol-validatedstorageadapterport-find-completed-write.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.list_segments](riverhog-storage-adapter-protocol-validatedstorageadapterport-list-segments.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.cleanup_read](riverhog-storage-adapter-protocol-validatedstorageadapterport-cleanup-read.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.write_segment](riverhog-storage-adapter-protocol-validatedstorageadapterport-write-segment.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.read_object](riverhog-storage-adapter-protocol-validatedstorageadapterport-read-object.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.delete_object](riverhog-storage-adapter-protocol-validatedstorageadapterport-delete-object.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.descriptor](riverhog-storage-adapter-protocol-validatedstorageadapterport-descriptor.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.delete_prefix](riverhog-storage-adapter-protocol-validatedstorageadapterport-delete-prefix.md)
- [riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.begin_write](riverhog-storage-adapter-protocol-validatedstorageadapterport-begin-write.md)

## Governing policies

- <a id="pa-115ed80630"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 26b6e734bdd0142520d9c744077f770b97340a50c866149dc221e3322587eef0 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(adapter: 'StorageAdapterPort') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ValidatedStorageAdapterPort",
  "unit": "export"
}
```
