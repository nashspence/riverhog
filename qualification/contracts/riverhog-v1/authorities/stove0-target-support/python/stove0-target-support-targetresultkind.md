# stove0_target_support.TargetResultKind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetresultkind:0cd0e8b4cd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-035089216d"></a>
| Field | Shape |
|---|---|
| <a id="s-36b59e7322"></a>`contract` | type="typing._LiteralGenericAlias"; additional keys=`kind` |
| <a id="s-cd8061d468"></a>`distribution` | "stove0-target-support" |
| <a id="s-c62fd172b4"></a>`module` | "stove0_target_support" |
| <a id="s-a919fbd232"></a>`name` | "TargetResultKind" |
| <a id="s-933be4afd9"></a>`unit` | "export" |

## Governing policies

- <a id="pa-dd2adac26a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetResultKind`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5116ff5aee4e06a71963f05df8310a9372ba16ea0ddd0798d08c376df8175179 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetResultKind",
  "unit": "export"
}
```
