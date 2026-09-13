# stove0_target_support.TargetExecutionCanceled

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutioncanceled:2d7f631765 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-852bc91ac6"></a>
| Field | Shape |
|---|---|
| <a id="s-0b4db0bbad"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-6f7f27ae79"></a>`distribution` | "stove0-target-support" |
| <a id="s-215a4c2d87"></a>`module` | "stove0_target_support" |
| <a id="s-29b86f592a"></a>`name` | "TargetExecutionCanceled" |
| <a id="s-bbad5258ca"></a>`unit` | "export" |

## Governing policies

- <a id="pa-650c66c85b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionCanceled`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b63fad6cb6e66fa3ed07b092738d501770b7731e0eebd2ab984df3874d4a426 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "unavailable"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetExecutionCanceled",
  "unit": "export"
}
```
