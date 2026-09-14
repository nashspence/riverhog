# riverhog_storage_adapter_protocol.validate_read_status_response

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-37599da0e6:85dcb60454 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-89ca8a2003"></a>
- <a id="s-45f1dd0b4b"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-7473490d2a"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-e310dccf55"></a>`name`: `validate_read_status_response`
- <a id="s-983f1a870b"></a>`unit`: `export`

### Declared structure

- <a id="s-c0b37185de"></a>`kind`: `"function"`
- <a id="s-47b902f6fb"></a>`signature`: `"\"(request: 'ReadPreparationRequest', response: 'ReadStatus') -> 'None'\""`

## Governing policies

- <a id="pa-d09ea007c0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_read_status_response`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 541fc7f8251906fff1e950cf0cebb75ad4ba5da74916c86167c52868c5d6fc84 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'ReadPreparationRequest', response: 'ReadStatus') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_read_status_response",
  "unit": "export"
}
```
