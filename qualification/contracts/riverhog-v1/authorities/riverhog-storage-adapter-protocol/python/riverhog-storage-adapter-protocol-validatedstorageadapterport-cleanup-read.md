# riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.cleanup_read

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-6ae4e3dd91:8081f48e98 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0160d29452"></a>
- <a id="s-2988b889c5"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-59e44d1497"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-d4a581ec6c"></a>`name`: `cleanup_read`
- <a id="s-2a3b3b34ad"></a>`owner`: `riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort`
- <a id="s-26d24e05a3"></a>`unit`: `member`

### Declared structure

- <a id="s-d77b0791f2"></a>`kind`: `"method"`
- <a id="s-4c117e887f"></a>`signature`: `"\"(self, request: 'ReadPreparationRequest') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ValidatedStorageAdapterPort](riverhog-storage-adapter-protocol-validatedstorageadapterport.md)

## Governing policies

- <a id="pa-f65a548c4a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort.cleanup_read`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac206344ca3c388a2bef5ff92456fa877dd9e435c58d605adc90ce0f1461772d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ReadPreparationRequest') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "cleanup_read",
  "owner": "riverhog_storage_adapter_protocol.ValidatedStorageAdapterPort",
  "unit": "member"
}
```

</details>
