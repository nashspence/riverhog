# stove0_target_support.TargetExecutionRuntime.completed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionrunt-b355333fd1:1d696e22ab -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eb46f04584"></a>
| Field | Shape |
|---|---|
| <a id="s-175e2be098"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-bf7029e6a0"></a>`distribution` | "stove0-target-support" |
| <a id="s-abd678653f"></a>`module` | "stove0_target_support" |
| <a id="s-c2b049b874"></a>`name` | "completed" |
| <a id="s-9049e06ab5"></a>`owner` | "stove0_target_support.TargetExecutionRuntime" |
| <a id="s-b4b5b621ab"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetExecutionRuntime](stove0-target-support-targetexecutionruntime.md)

## Governing policies

- <a id="pa-3a1df994dc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionRuntime.completed`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f9499903c2d962d8aaf78a3fde948412676df4fa19715218f707e840d0999a9 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'bool'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "completed",
  "owner": "stove0_target_support.TargetExecutionRuntime",
  "unit": "member"
}
```
