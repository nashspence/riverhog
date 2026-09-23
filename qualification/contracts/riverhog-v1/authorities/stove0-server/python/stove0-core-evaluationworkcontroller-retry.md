# stove0_core.EvaluationWorkController.retry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationworkcontroller-retry:b1d8298b16 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b50c354db5"></a>
- <a id="s-a4d9d1a8f4"></a>`distribution`: `stove0-server`
- <a id="s-63919bfe8a"></a>`module`: `stove0_core`
- <a id="s-5397a51861"></a>`name`: `retry`
- <a id="s-b63a488234"></a>`owner`: `stove0_core.EvaluationWorkController`
- <a id="s-ea65317fc1"></a>`unit`: `member`

### Declared structure

- <a id="s-f285d81fae"></a>`kind`: `"method"`
- <a id="s-ead6c22db0"></a>`signature`: `"\"(self, work_id: 'str') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [EvaluationWorkController](stove0-core-evaluationworkcontroller.md)

## Governing policies

- <a id="pa-95aea13298"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationWorkController.retry`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce95286bc1f1e4bf64d9cebf9d9ce6d38e9d1cdee5a45c646a58e615819ffa09 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retry",
  "owner": "stove0_core.EvaluationWorkController",
  "unit": "member"
}
```

</details>
