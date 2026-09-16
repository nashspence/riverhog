# riverhog_storage_adapter_protocol.StorageAdapterPort.delete_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-610f138d36:961fc01178 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-01c3658736"></a>
- <a id="s-f42bc968ae"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-a97d34b013"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-a8d02034b9"></a>`name`: `delete_object`
- <a id="s-11f035ca41"></a>`owner`: `riverhog_storage_adapter_protocol.StorageAdapterPort`
- <a id="s-5afbb94772"></a>`unit`: `member`

### Declared structure

- <a id="s-076ee26db3"></a>`kind`: `"method"`
- <a id="s-8d73febdca"></a>`signature`: `"\"(self, request: 'DeleteObjectRequest') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterPort](riverhog-storage-adapter-protocol-storageadapterport.md)

## Governing policies

- <a id="pa-144a4da8fa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterPort.delete_object`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e1f0290493cefdffab6bec1ff15c316dd07aadcb0eae8d411a75e237d7e5a93 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'DeleteObjectRequest') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "delete_object",
  "owner": "riverhog_storage_adapter_protocol.StorageAdapterPort",
  "unit": "member"
}
```

</details>
