# stove0_operator_contracts.validate_work_state_shape

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-validate-work-state-shape:6cd2718610 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-12abe81c0c"></a>
- <a id="s-8a7babf842"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-a80be1d20f"></a>`module`: `stove0_operator_contracts`
- <a id="s-35d03d716e"></a>`name`: `validate_work_state_shape`
- <a id="s-bfb85d2ada"></a>`unit`: `export`

### Declared structure

- <a id="s-89029571f7"></a>`kind`: `"function"`
- <a id="s-014acc5ea3"></a>`signature`: `"'(*, work: \\'WorkIdentity\\', phase: \\'WorkPhase\\', claim: \\'object \| None\\', preview_acceptance: \\'object \| None\\', expected_target_plan_sha256: \\'str \| None\\', branch_set_plan: \\'BranchSetPlan \| None\\', coordination_settlement: \\'CoordinationSettlement \| None\\', join_plan: \\'JoinPlan \| None\\', coordination_cancel_requested: \\'bool\\', workflow_plan: \\'WorkflowPlan \| None\\', output: \\'OutputCollectionRef \| None\\', retirement_remaining: \\'Sequence[int]\\', failure: \\'object \| None\\', inapplicable: \\'object \| None\\', abandon_outcome: \"Literal[\\'inapplicable\\', \\'failed\\', \\'canceled\\'] \| None\") -> \\'None\\''"`

## Governing policies

- <a id="pa-1f3d29a784"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.validate_work_state_shape`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2c1ab38f3d0a8540756a3594ff1dad20dfbd46936b5f905510073f23bef1fde6 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "'(*, work: \\'WorkIdentity\\', phase: \\'WorkPhase\\', claim: \\'object | None\\', preview_acceptance: \\'object | None\\', expected_target_plan_sha256: \\'str | None\\', branch_set_plan: \\'BranchSetPlan | None\\', coordination_settlement: \\'CoordinationSettlement | None\\', join_plan: \\'JoinPlan | None\\', coordination_cancel_requested: \\'bool\\', workflow_plan: \\'WorkflowPlan | None\\', output: \\'OutputCollectionRef | None\\', retirement_remaining: \\'Sequence[int]\\', failure: \\'object | None\\', inapplicable: \\'object | None\\', abandon_outcome: \"Literal[\\'inapplicable\\', \\'failed\\', \\'canceled\\'] | None\") -> \\'None\\''"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "validate_work_state_shape",
  "unit": "export"
}
```
