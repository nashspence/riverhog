# stove0_core.EvaluationWorkController.step

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationworkcontroller-step:d3827cf292 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4e5a3662b9"></a>
- <a id="s-ce9357c07f"></a>`distribution`: `stove0-server`
- <a id="s-ff2f6e2313"></a>`module`: `stove0_core`
- <a id="s-aa1cfbba8d"></a>`name`: `step`
- <a id="s-7868a7d065"></a>`owner`: `stove0_core.EvaluationWorkController`
- <a id="s-d661caba41"></a>`unit`: `member`

### Declared structure

- <a id="s-5c02b4a6ed"></a>`kind`: `"method"`
- <a id="s-e831eb8dde"></a>`signature`: `"\"(self, work_id: 'str') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [EvaluationWorkController](stove0-core-evaluationworkcontroller.md)

## Governing policies

- <a id="pa-ebe5928c00"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationWorkController.step`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52910db688482617dcd713a5b7395d803472b20814033dd5b61d587fc6519991 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "step",
  "owner": "stove0_core.EvaluationWorkController",
  "unit": "member"
}
```

</details>
