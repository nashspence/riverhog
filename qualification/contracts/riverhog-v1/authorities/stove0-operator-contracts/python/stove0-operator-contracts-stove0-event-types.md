# stove0_operator_contracts.STOVE0_EVENT_TYPES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0-event-types:b558cb53f5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-027e2be7a8"></a>
| Field | Shape |
|---|---|
| <a id="s-42dd445196"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-573b0520bc"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-6e0621fb4b"></a>`module` | "stove0_operator_contracts" |
| <a id="s-c68643ef72"></a>`name` | "STOVE0_EVENT_TYPES" |
| <a id="s-127c1eac71"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d2112f137d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.STOVE0_EVENT_TYPES`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 610fc17550df8cd54cebfeedfd38280a369299568c5abb118938fc9ba4483d47 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": [
      "io.riverhog.stove0.branch-set.admitted",
      "io.riverhog.stove0.evaluation.created",
      "io.riverhog.stove0.evaluation.updated",
      "io.riverhog.stove0.join.admitted",
      "io.riverhog.stove0.work.created",
      "io.riverhog.stove0.work.updated"
    ]
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "STOVE0_EVENT_TYPES",
  "unit": "export"
}
```
