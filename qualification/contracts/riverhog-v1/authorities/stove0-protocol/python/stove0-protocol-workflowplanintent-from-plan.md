# stove0_protocol.WorkflowPlanIntent.from_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplanintent-from-plan:d8b3d4397b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a7cb28e0fc"></a>
- <a id="s-aec98793e8"></a>`distribution`: `stove0-protocol`
- <a id="s-e8a9ad7ead"></a>`module`: `stove0_protocol`
- <a id="s-71684bfc13"></a>`name`: `from_plan`
- <a id="s-d8b1833a73"></a>`owner`: `stove0_protocol.WorkflowPlanIntent`
- <a id="s-030c333831"></a>`unit`: `member`

### Declared structure

- <a id="s-950f15f856"></a>`kind`: `"classmethod"`
- <a id="s-279ce2593f"></a>`signature`: `"\"(cls, plan: 'WorkflowPlan') -> 'WorkflowPlanIntent'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPlanIntent](stove0-protocol-workflowplanintent.md)

## Governing policies

- <a id="pa-478129901d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlanIntent.from_plan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebb878d51445c9b1af4558b189b2b41d18f18e0882ec7ca5d52d135818cfbfd4 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, plan: 'WorkflowPlan') -> 'WorkflowPlanIntent'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "from_plan",
  "owner": "stove0_protocol.WorkflowPlanIntent",
  "unit": "member"
}
```

</details>
