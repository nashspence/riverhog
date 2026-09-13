# riverhog_storage_adapter_support.FRAMED_BODY_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-framed-body-format:2ae3c30814 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3a55e34712"></a>
| Field | Shape |
|---|---|
| <a id="s-030252b703"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-d750213a8b"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-4aa8114566"></a>`module` | "riverhog_storage_adapter_support" |
| <a id="s-4e92a28835"></a>`name` | "FRAMED_BODY_FORMAT" |
| <a id="s-b59c0091b1"></a>`unit` | "export" |

## Governing policies

- <a id="pa-ac3a8d3a97"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.FRAMED_BODY_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8e85a95804b8b5363d7be50dc0e7882471272dc1a0c46dcd3386db3d53fa2f7a -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-json-opaque-framing/v1"
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "FRAMED_BODY_FORMAT",
  "unit": "export"
}
```
