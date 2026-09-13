# riverhog_storage_adapter_support.STORAGE_ADAPTER_CONFORMANCE_RESULT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storage-30c1dcd9d4:a8fe3a9245 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8c3ba82b74"></a>
| Field | Shape |
|---|---|
| <a id="s-b60a7d30dd"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-f4f18dfff5"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-eed9eeea5e"></a>`module` | "riverhog_storage_adapter_support" |
| <a id="s-27f4620606"></a>`name` | "STORAGE_ADAPTER_CONFORMANCE_RESULT" |
| <a id="s-87afb39c62"></a>`unit` | "export" |

## Governing policies

- <a id="pa-0181676391"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.STORAGE_ADAPTER_CONFORMANCE_RESULT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1220a30cf79c090b49ee6d3c5e4525ce039d7a39c170d07c510dff59a7bcf439 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-storage-adapter-conformance-result/v1"
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "STORAGE_ADAPTER_CONFORMANCE_RESULT",
  "unit": "export"
}
```
