# riverhog_storage_adapter_protocol.StorageAdapterPort.find_completed_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-2b999029c3:19b04b771c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7d3b927868"></a>
- <a id="s-63090e93d3"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-88534b28bf"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-bbca9f7275"></a>`name`: `find_completed_write`
- <a id="s-3734a4c123"></a>`owner`: `riverhog_storage_adapter_protocol.StorageAdapterPort`
- <a id="s-d34a1213f5"></a>`unit`: `member`

### Declared structure

- <a id="s-c3cde93168"></a>`kind`: `"method"`
- <a id="s-8dfae1f67a"></a>`signature`: `"\"(self, request: 'CompletedWriteLookupRequest') -> 'CompletedObjectReceipt \| None'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterPort](riverhog-storage-adapter-protocol-storageadapterport.md)

## Governing policies

- <a id="pa-aa5d08d3f7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterPort.find_completed_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88d8d86293ebf9f201b8ff26716cddbce75e388069f6521ff35513bd9cc0fbef -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'CompletedWriteLookupRequest') -> 'CompletedObjectReceipt | None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "find_completed_write",
  "owner": "riverhog_storage_adapter_protocol.StorageAdapterPort",
  "unit": "member"
}
```

</details>
