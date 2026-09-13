# stove0_target_protocol.TargetPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetplan:413a256b8f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0f0f01d724"></a>
| Field | Shape |
|---|---|
| <a id="s-6e2e6f5f98"></a>`contract` | type="typing._AnnotatedAlias"; additional keys=`kind` |
| <a id="s-ce369fe8ea"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-caeb64852e"></a>`module` | "stove0_target_protocol" |
| <a id="s-b33525b3ee"></a>`name` | "TargetPlan" |
| <a id="s-67658fb177"></a>`unit` | "export" |

## Governing policies

- <a id="pa-2c1e1425bb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetPlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4755ebefd7dfda9c88abfe2e40ee921f3a52e998b36a4ee930ec8bc15872340c -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetPlan",
  "unit": "export"
}
```
