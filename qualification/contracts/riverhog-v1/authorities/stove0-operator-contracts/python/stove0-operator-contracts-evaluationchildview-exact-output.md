# stove0_operator_contracts.EvaluationChildView.exact_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationchild-c1ca56a9b3:7faf51083b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3c86b1eaa7"></a>
| Field | Shape |
|---|---|
| <a id="s-206b5cd42f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-2773e9bfae"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-e27d078d25"></a>`module` | "stove0_operator_contracts" |
| <a id="s-44dd7b35af"></a>`name` | "exact_output" |
| <a id="s-b8455a3ca8"></a>`owner` | "stove0_operator_contracts.EvaluationChildView" |
| <a id="s-edcf734931"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.EvaluationChildView](stove0-operator-contracts-evaluationchildview.md)

## Governing policies

- <a id="pa-b0621c1954"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationChildView.exact_output`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd706c91b9fb260aa4c903eb2d00683a3b2be6b2e44e71638041e33ff84b7726 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "exact_output",
  "owner": "stove0_operator_contracts.EvaluationChildView",
  "unit": "member"
}
```
