# gogurt_core.GOGURT_PROVIDER_REFERENCE_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-gogurt-provider-reference-format:defd73b98f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7f28e0f4e"></a>
| Field | Shape |
|---|---|
| <a id="s-47d2051d66"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-8b9b46d5b6"></a>`distribution` | "gogurt-core" |
| <a id="s-461aeea837"></a>`module` | "gogurt_core" |
| <a id="s-df5702ef7d"></a>`name` | "GOGURT_PROVIDER_REFERENCE_FORMAT" |
| <a id="s-ab14ee8885"></a>`unit` | "export" |

## Governing policies

- <a id="pa-afd89a8566"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.GOGURT_PROVIDER_REFERENCE_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: feb8fa7b0f3508fada0e08c29361225365ff13e0bfa0a7dc2c77cd4e19b44d64 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-provider-reference/v1"
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "GOGURT_PROVIDER_REFERENCE_FORMAT",
  "unit": "export"
}
```
