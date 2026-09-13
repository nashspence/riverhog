# stove0_operator_contracts.parse_stove0_event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-parse-stove0-event:3bc9b830e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-280b6c3965"></a>
| Field | Shape |
|---|---|
| <a id="s-c5f4685768"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-58df574e68"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-0a3834d67a"></a>`module` | "stove0_operator_contracts" |
| <a id="s-8dfc13abff"></a>`name` | "parse_stove0_event" |
| <a id="s-7d85e7d3e7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-77c76f75a1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.parse_stove0_event`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75dd0a0a300dfc2dd69fafbb5532298847f79ba4c0135a618eee110b62fc593a -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'Stove0LifecycleEvent'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "parse_stove0_event",
  "unit": "export"
}
```
