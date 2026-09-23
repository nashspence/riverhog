# stove0_core.EvaluationWorkController.cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationworkcontroller-cancel:0dfc304e7b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f4f13fdb8a"></a>
- <a id="s-b75ba10a85"></a>`distribution`: `stove0-server`
- <a id="s-7f0b179d44"></a>`module`: `stove0_core`
- <a id="s-2cf6a4bae2"></a>`name`: `cancel`
- <a id="s-858341cc73"></a>`owner`: `stove0_core.EvaluationWorkController`
- <a id="s-827913482e"></a>`unit`: `member`

### Declared structure

- <a id="s-9c9dbc7dc7"></a>`kind`: `"method"`
- <a id="s-0d5d4be964"></a>`signature`: `"\"(self, work_id: 'str') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [EvaluationWorkController](stove0-core-evaluationworkcontroller.md)

## Governing policies

- <a id="pa-1685cb17a1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationWorkController.cancel`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f5e99d20799097083ac57f478f3ecc499c61f615011e935a01ae0c4b4f59587 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "cancel",
  "owner": "stove0_core.EvaluationWorkController",
  "unit": "member"
}
```

</details>
