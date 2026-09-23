# stove0_protocol.WorkflowPlanPayload.protect_evaluation_sources

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplanpayload-prote-29f2b2ba46:051156cae0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a2b35b7d0d"></a>
- <a id="s-eac11e2bd9"></a>`distribution`: `stove0-protocol`
- <a id="s-9f0aaf7ce4"></a>`module`: `stove0_protocol`
- <a id="s-d27f305a2c"></a>`name`: `protect_evaluation_sources`
- <a id="s-175f92c574"></a>`owner`: `stove0_protocol.WorkflowPlanPayload`
- <a id="s-52f3862174"></a>`unit`: `member`

### Declared structure

- <a id="s-efbef9b463"></a>`kind`: `"method"`
- <a id="s-532a6ddd39"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPlanPayload](stove0-protocol-workflowplanpayload.md)

## Governing policies

- <a id="pa-4cf2d5122e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlanPayload.protect_evaluation_sources`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd1591c09b36d36bf7bba119c48af53f077446de2d1f488eaee9eb01ad2dd2ab -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "protect_evaluation_sources",
  "owner": "stove0_protocol.WorkflowPlanPayload",
  "unit": "member"
}
```

</details>
