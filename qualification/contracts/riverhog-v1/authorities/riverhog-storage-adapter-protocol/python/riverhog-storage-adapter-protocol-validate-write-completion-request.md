# riverhog_storage_adapter_protocol.validate_write_completion_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-17b36214ad:11f1c399ca -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fd3258e513"></a>
| Field | Shape |
|---|---|
| <a id="s-9ae4baf619"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-fdf176740a"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-5f02d1d040"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-0654370e7c"></a>`name` | "validate_write_completion_request" |
| <a id="s-058cce421f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-54c8c2e690"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_write_completion_request`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d25c5c7dde50eb6bdff0092d394eb6c340a640697c159a83d8d391c21e65f92 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'WriteCompleteRequest', descriptor: 'AdapterDescriptor') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_write_completion_request",
  "unit": "export"
}
```
