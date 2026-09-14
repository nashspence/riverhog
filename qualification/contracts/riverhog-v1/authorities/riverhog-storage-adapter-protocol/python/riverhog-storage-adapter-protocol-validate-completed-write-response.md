# riverhog_storage_adapter_protocol.validate_completed_write_response

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-6ca6636ebf:3783f3a905 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ae68f88f4f"></a>
- <a id="s-28f75526e5"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-0d0eac06c4"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-e014340917"></a>`name`: `validate_completed_write_response`
- <a id="s-c31189c986"></a>`unit`: `export`

### Declared structure

- <a id="s-84fde62e0c"></a>`kind`: `"function"`
- <a id="s-87137176b4"></a>`signature`: `"\"(request: 'WriteCompleteRequest \| CompletedWriteLookupRequest', response: 'CompletedObjectReceipt') -> 'None'\""`

## Governing policies

- <a id="pa-e55b55c111"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_completed_write_response`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eda4b30d9b57d6f6668c3da63d861abd7d367aa41a3385682130d8c6f489315c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'WriteCompleteRequest | CompletedWriteLookupRequest', response: 'CompletedObjectReceipt') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_completed_write_response",
  "unit": "export"
}
```
