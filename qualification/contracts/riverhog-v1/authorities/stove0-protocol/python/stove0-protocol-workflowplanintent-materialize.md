# stove0_protocol.WorkflowPlanIntent.materialize

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplanintent-materialize:c4325270ce -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-877d2dd0a3"></a>
- <a id="s-5c8bbfa5ce"></a>`distribution`: `stove0-protocol`
- <a id="s-3e180c16f0"></a>`module`: `stove0_protocol`
- <a id="s-fb0dbe7feb"></a>`name`: `materialize`
- <a id="s-bde966a94e"></a>`owner`: `stove0_protocol.WorkflowPlanIntent`
- <a id="s-1a7441c936"></a>`unit`: `member`

### Declared structure

- <a id="s-e7e2b961da"></a>`kind`: `"method"`
- <a id="s-e9530dca67"></a>`signature`: `"\"(self, *, work: 'WorkIdentity', observations: 'tuple[ContentObservationEvidence, ...]' = ()) -> 'WorkflowPlan'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPlanIntent](stove0-protocol-workflowplanintent.md)

## Governing policies

- <a id="pa-86ba39defa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlanIntent.materialize`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dccdbd41d91348b4c8b7146f60070e2c36addf007d68d501f1b44f6046e68d4e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, work: 'WorkIdentity', observations: 'tuple[ContentObservationEvidence, ...]' = ()) -> 'WorkflowPlan'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "materialize",
  "owner": "stove0_protocol.WorkflowPlanIntent",
  "unit": "member"
}
```

</details>
