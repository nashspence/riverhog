# riverhog_storage_adapter_support.FRAMED_STORAGE_ADAPTER_HTTP_PATHS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-framed-s-3b656667f5:ee30f41d20 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b9775af6c4"></a>
| Field | Shape |
|---|---|
| <a id="s-1072f02cd0"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-8d6aa5452c"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-8a00dc8361"></a>`module` | "riverhog_storage_adapter_support" |
| <a id="s-a7c3c83b4c"></a>`name` | "FRAMED_STORAGE_ADAPTER_HTTP_PATHS" |
| <a id="s-3e44926ca9"></a>`unit` | "export" |

## Governing policies

- <a id="pa-dcd363420a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.FRAMED_STORAGE_ADAPTER_HTTP_PATHS`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d2ff0f43853c3f9cf54442ccb2807d72d5749ce65aad4105bbec6caec1baa5a5 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": [
      "/v1/objects/put",
      "/v1/writes/segment"
    ]
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "FRAMED_STORAGE_ADAPTER_HTTP_PATHS",
  "unit": "export"
}
```
