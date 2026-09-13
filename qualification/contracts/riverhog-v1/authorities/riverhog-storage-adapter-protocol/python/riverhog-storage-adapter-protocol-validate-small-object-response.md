# riverhog_storage_adapter_protocol.validate_small_object_response

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-2236747fb3:fe3776d18c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-338b52e453"></a>
| Field | Shape |
|---|---|
| <a id="s-e6f77b5861"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-5e7b91baa4"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-cb3cc79b91"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-c3943558b2"></a>`name` | "validate_small_object_response" |
| <a id="s-cff223a14c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-b717c7f565"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_small_object_response`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df7f7dce1e887bcfb62a865832f33bba478ac862bebbc48c9a2a0465949b593d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'SmallObjectWriteRequest', response: 'ImmutableObjectReceipt') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_small_object_response",
  "unit": "export"
}
```
