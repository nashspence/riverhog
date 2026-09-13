# riverhog_storage_adapter_support.STORAGE_ADAPTER_SCHEMA_BUNDLE_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storage-aa65cefaf3:3ef87fb6f0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b29555100f"></a>
| Field | Shape |
|---|---|
| <a id="s-91fc64dc3f"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-db1d670ef6"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-f48ebd394d"></a>`module` | "riverhog_storage_adapter_support" |
| <a id="s-05f0444738"></a>`name` | "STORAGE_ADAPTER_SCHEMA_BUNDLE_FORMAT" |
| <a id="s-5f144404bf"></a>`unit` | "export" |

## Governing policies

- <a id="pa-ba2026664f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.STORAGE_ADAPTER_SCHEMA_BUNDLE_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d9f12b17c66c21bb7e6b213c90349932c80f95ba645e8509bddb01513eea42a3 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-storage-adapter-schema-bundle/v1"
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "STORAGE_ADAPTER_SCHEMA_BUNDLE_FORMAT",
  "unit": "export"
}
```
