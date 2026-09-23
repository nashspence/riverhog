# stove0_protocol.WorkflowPlan.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplan-verify-digest:92c3eb8fd2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-82ef7e1c8f"></a>
- <a id="s-f0b8a1c565"></a>`distribution`: `stove0-protocol`
- <a id="s-aeb7a23c2d"></a>`module`: `stove0_protocol`
- <a id="s-3e82db6800"></a>`name`: `verify_digest`
- <a id="s-6534b865ca"></a>`owner`: `stove0_protocol.WorkflowPlan`
- <a id="s-e0c0acb46f"></a>`unit`: `member`

### Declared structure

- <a id="s-01f4b89226"></a>`kind`: `"method"`
- <a id="s-046aeb3532"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPlan](stove0-protocol-workflowplan.md)

## Governing policies

- <a id="pa-3407ac3983"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlan.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8701735e19b4340ef160c01c688ed806ebbc269527b5caf244e9a6b04ec3681b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_digest",
  "owner": "stove0_protocol.WorkflowPlan",
  "unit": "member"
}
```

</details>
