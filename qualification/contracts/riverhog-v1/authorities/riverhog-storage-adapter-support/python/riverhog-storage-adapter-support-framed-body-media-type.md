# riverhog_storage_adapter_support.FRAMED_BODY_MEDIA_TYPE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-framed-b-c545cbe7e4:2ba767bbdc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7b96d315c2"></a>
| Field | Shape |
|---|---|
| <a id="s-1d9dfaac3a"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-8e79b16ab4"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-e6c1ad0889"></a>`module` | "riverhog_storage_adapter_support" |
| <a id="s-2f200311a8"></a>`name` | "FRAMED_BODY_MEDIA_TYPE" |
| <a id="s-477ac39079"></a>`unit` | "export" |

## Governing policies

- <a id="pa-982317a740"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.FRAMED_BODY_MEDIA_TYPE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9379d07f6813bb7d3cb7c839a9e1f6a759f3261b14df1b223b6be4e53aecad93 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "application/vnd.riverhog.json-opaque-framing"
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "FRAMED_BODY_MEDIA_TYPE",
  "unit": "export"
}
```
