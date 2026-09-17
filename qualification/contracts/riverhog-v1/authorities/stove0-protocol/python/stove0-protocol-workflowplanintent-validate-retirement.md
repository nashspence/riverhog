# stove0_protocol.WorkflowPlanIntent.validate_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowplanintent-valida-94ca29f8cb:ed517e709e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-37255fc6a8"></a>
- <a id="s-752b74070e"></a>`distribution`: `stove0-protocol`
- <a id="s-c9bb04b42c"></a>`module`: `stove0_protocol`
- <a id="s-9dec4ef7ba"></a>`name`: `validate_retirement`
- <a id="s-9316f86443"></a>`owner`: `stove0_protocol.WorkflowPlanIntent`
- <a id="s-221242432c"></a>`unit`: `member`

### Declared structure

- <a id="s-6b32a8d2a1"></a>`kind`: `"method"`
- <a id="s-bde2371621"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPlanIntent](stove0-protocol-workflowplanintent.md)

## Governing policies

- <a id="pa-24feec638a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPlanIntent.validate_retirement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a131b30cb35af4a2fd5480790faf9ed8e404dbdfc5e3f1d5a765f9263700b5d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "validate_retirement",
  "owner": "stove0_protocol.WorkflowPlanIntent",
  "unit": "member"
}
```

</details>
