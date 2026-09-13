# riverhog_storage_adapter_protocol.ADAPTER_PRIVATE_ASSERTION_PREFIX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-adapter-01114f2068:3566cdef83 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9ef9cc904b"></a>
| Field | Shape |
|---|---|
| <a id="s-cef1779069"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-13c96768ee"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-3143b8be61"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-d6f26601cc"></a>`name` | "ADAPTER_PRIVATE_ASSERTION_PREFIX" |
| <a id="s-8f9b857bcf"></a>`unit` | "export" |

## Governing policies

- <a id="pa-eccbee5ed6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ADAPTER_PRIVATE_ASSERTION_PREFIX`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5413e9479b811bbf2f9062ede2c094cfe1a5fbc3c26f50bf12428d7689f64ba9 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-adapter-"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ADAPTER_PRIVATE_ASSERTION_PREFIX",
  "unit": "export"
}
```
