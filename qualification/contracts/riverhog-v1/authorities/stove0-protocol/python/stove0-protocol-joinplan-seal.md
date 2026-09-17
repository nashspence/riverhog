# stove0_protocol.JoinPlan.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinplan-seal:74b573fb00 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6687a1fdfa"></a>
- <a id="s-ef7ab9a008"></a>`distribution`: `stove0-protocol`
- <a id="s-b4d3eaacaa"></a>`module`: `stove0_protocol`
- <a id="s-3f155a0b5d"></a>`name`: `seal`
- <a id="s-18a987bd93"></a>`owner`: `stove0_protocol.JoinPlan`
- <a id="s-2c843de7d4"></a>`unit`: `member`

### Declared structure

- <a id="s-380d7f519f"></a>`kind`: `"classmethod"`
- <a id="s-138e6e9cec"></a>`signature`: `"\"(cls, *, parent_work_id: 'str', branch_set_sha256: 'str', declaration: 'JoinDeclaration', inputs: 'Sequence[JoinInputPlan]', work: 'WorkIdentity', workflow_plan: 'WorkflowPlan') -> 'JoinPlan'\""`

## Maintained corroboration

### Related interface records

- [JoinPlan](stove0-protocol-joinplan.md)

## Governing policies

- <a id="pa-4d425f212b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JoinPlan.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a790a3ea659f7b12af230bdf24b613b96acc960d569da00b696d437af934321e -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, parent_work_id: 'str', branch_set_sha256: 'str', declaration: 'JoinDeclaration', inputs: 'Sequence[JoinInputPlan]', work: 'WorkIdentity', workflow_plan: 'WorkflowPlan') -> 'JoinPlan'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.JoinPlan",
  "unit": "member"
}
```

</details>
