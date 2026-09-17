# stove0_protocol.BranchPlan.build

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchplan-build:e06239f04a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7d6b4d93ff"></a>
- <a id="s-b832fac70b"></a>`distribution`: `stove0-protocol`
- <a id="s-2204054957"></a>`module`: `stove0_protocol`
- <a id="s-7b5b89469f"></a>`name`: `build`
- <a id="s-1393b7e4f1"></a>`owner`: `stove0_protocol.BranchPlan`
- <a id="s-3b9c0d64c9"></a>`unit`: `member`

### Declared structure

- <a id="s-796f2170ea"></a>`kind`: `"classmethod"`
- <a id="s-7a25d38b50"></a>`signature`: `"\"(cls, *, parent_work: 'WorkIdentity', branch_id: 'str', decision_sha256: 'str', selection: 'ArtifactSelection', recipe: 'RecipeRef', effective_intent: 'Mapping[str, JsonValue]', workflow_intent: 'WorkflowPlanIntent', observations: 'tuple[ObservationEvidence, ...]' = ()) -> 'BranchPlan'\""`

## Maintained corroboration

### Related interface records

- [BranchPlan](stove0-protocol-branchplan.md)

## Governing policies

- <a id="pa-2a84c19c15"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchPlan.build`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9fbb502dea378154b7c7261752c7d32b683a93b0670f53d7b7b138f3965a3055 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, parent_work: 'WorkIdentity', branch_id: 'str', decision_sha256: 'str', selection: 'ArtifactSelection', recipe: 'RecipeRef', effective_intent: 'Mapping[str, JsonValue]', workflow_intent: 'WorkflowPlanIntent', observations: 'tuple[ObservationEvidence, ...]' = ()) -> 'BranchPlan'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "build",
  "owner": "stove0_protocol.BranchPlan",
  "unit": "member"
}
```

</details>
