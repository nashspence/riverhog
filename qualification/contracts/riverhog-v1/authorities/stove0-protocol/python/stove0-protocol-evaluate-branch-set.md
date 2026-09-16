# stove0_protocol.evaluate_branch_set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-evaluate-branch-set:3c4791044d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b31859ea0c"></a>
- <a id="s-19cb811b5d"></a>`distribution`: `stove0-protocol`
- <a id="s-25f2893bd3"></a>`module`: `stove0_protocol`
- <a id="s-53dc57fd2a"></a>`name`: `evaluate_branch_set`
- <a id="s-03789c8702"></a>`unit`: `export`

### Declared structure

- <a id="s-7f6e4041cc"></a>`kind`: `"function"`
- <a id="s-e2c728cc7e"></a>`signature`: `"\"(plan: 'BranchSetPlan', selections: 'SelectionDocuments', *, branch_sets: 'Mapping[str, BranchSetPlan] \| None' = None, branch_settlements: 'Sequence[BranchSettlement]' = (), branch_effect_settlements: 'Sequence[BranchEffectSettlement]' = (), branch_coordination_settlements: 'Sequence[CoordinationSettlement]' = (), branch_outcomes: 'Sequence[BranchOutcome]' = (), join_settlement: 'JoinSettlement \| None' = None, join_outcome: 'JoinOutcome \| None' = None) -> 'BranchSetEvaluation'\""`

## Governing policies

- <a id="pa-e240030124"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.evaluate_branch_set`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 925568cf8a3a2b102c5729041eae19ed6de12638b4ce4afbe34cd4401111ddd4 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(plan: 'BranchSetPlan', selections: 'SelectionDocuments', *, branch_sets: 'Mapping[str, BranchSetPlan] | None' = None, branch_settlements: 'Sequence[BranchSettlement]' = (), branch_effect_settlements: 'Sequence[BranchEffectSettlement]' = (), branch_coordination_settlements: 'Sequence[CoordinationSettlement]' = (), branch_outcomes: 'Sequence[BranchOutcome]' = (), join_settlement: 'JoinSettlement | None' = None, join_outcome: 'JoinOutcome | None' = None) -> 'BranchSetEvaluation'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "evaluate_branch_set",
  "unit": "export"
}
```

</details>
