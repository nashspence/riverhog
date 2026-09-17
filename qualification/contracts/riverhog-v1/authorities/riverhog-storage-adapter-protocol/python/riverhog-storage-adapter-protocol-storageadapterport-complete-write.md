# riverhog_storage_adapter_protocol.StorageAdapterPort.complete_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-d72c0e893d:f82578358f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f63d6d86bf"></a>
- <a id="s-26d53da6dc"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-cdbd97e273"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-9854fdf7fd"></a>`name`: `complete_write`
- <a id="s-3bdd912ccc"></a>`owner`: `riverhog_storage_adapter_protocol.StorageAdapterPort`
- <a id="s-f52d9afef3"></a>`unit`: `member`

### Declared structure

- <a id="s-4df76cca68"></a>`kind`: `"method"`
- <a id="s-26cb5636f1"></a>`signature`: `"\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterPort](riverhog-storage-adapter-protocol-storageadapterport.md)

## Governing policies

- <a id="pa-18d390c1dd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterPort.complete_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 235b5febfc2609d87ee328246a77a9e3166e461303b357a04b6103defde93da9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteCompleteRequest') -> 'CompletedObjectReceipt'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "complete_write",
  "owner": "riverhog_storage_adapter_protocol.StorageAdapterPort",
  "unit": "member"
}
```

</details>
