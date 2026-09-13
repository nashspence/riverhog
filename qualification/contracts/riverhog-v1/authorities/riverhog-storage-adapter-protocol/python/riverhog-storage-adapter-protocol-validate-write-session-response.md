# riverhog_storage_adapter_protocol.validate_write_session_response

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-7af84bac69:7cdc34b6c6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0cb5b37259"></a>
| Field | Shape |
|---|---|
| <a id="s-1b50ae61c6"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-18a4a5cfa1"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-3dec63060f"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-2ea77ed422"></a>`name` | "validate_write_session_response" |
| <a id="s-317087902c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-6c03a491a5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_write_session_response`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29657afb16e7d50796735742ca0a2ecc579260a4351ecbdc52d9cb03de6ac802 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'WriteStartRequest', response: 'WriteSession') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_write_session_response",
  "unit": "export"
}
```
