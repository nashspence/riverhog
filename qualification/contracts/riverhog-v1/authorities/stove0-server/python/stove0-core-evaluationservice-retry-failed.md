# stove0_core.EvaluationService.retry_failed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationservice-retry-failed:79909e3306 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1680c7497a"></a>
- <a id="s-d4908b3958"></a>`distribution`: `stove0-server`
- <a id="s-549fd3c436"></a>`module`: `stove0_core`
- <a id="s-a907d34849"></a>`name`: `retry_failed`
- <a id="s-a1623170ed"></a>`owner`: `stove0_core.EvaluationService`
- <a id="s-8c1095f28d"></a>`unit`: `member`

### Declared structure

- <a id="s-276672e756"></a>`kind`: `"method"`
- <a id="s-6a5f93705b"></a>`signature`: `"\"(self, evaluation_id: 'str', variant_id: 'str', *, controller: 'EvaluationWorkController') -> 'EvaluationRecord'\""`

## Maintained corroboration

### Related interface records

- [EvaluationService](stove0-core-evaluationservice.md)

## Governing policies

- <a id="pa-d6cc526978"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationService.retry_failed`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ea2841ea6ca901538a1226ca113dde11716efe4f941b9ca09f4834803fddb7c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str', variant_id: 'str', *, controller: 'EvaluationWorkController') -> 'EvaluationRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retry_failed",
  "owner": "stove0_core.EvaluationService",
  "unit": "member"
}
```

</details>
