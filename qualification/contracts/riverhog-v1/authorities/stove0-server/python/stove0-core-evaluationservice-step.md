# stove0_core.EvaluationService.step

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationservice-step:8141ce3023 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b90d8dc636"></a>
- <a id="s-92d345fac1"></a>`distribution`: `stove0-server`
- <a id="s-264f1c09b6"></a>`module`: `stove0_core`
- <a id="s-21098eefd8"></a>`name`: `step`
- <a id="s-32ec4a22fa"></a>`owner`: `stove0_core.EvaluationService`
- <a id="s-f508173602"></a>`unit`: `member`

### Declared structure

- <a id="s-488e0f6783"></a>`kind`: `"method"`
- <a id="s-94dea1ed6a"></a>`signature`: `"\"(self, evaluation_id: 'str', *, controller: 'EvaluationWorkController') -> 'EvaluationRecord'\""`

## Maintained corroboration

### Related interface records

- [EvaluationService](stove0-core-evaluationservice.md)

## Governing policies

- <a id="pa-d246b27c7c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationService.step`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d37963f0101314d1126e49b94b97e793ec7a430d349533bd1854742657331e55 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str', *, controller: 'EvaluationWorkController') -> 'EvaluationRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "step",
  "owner": "stove0_core.EvaluationService",
  "unit": "member"
}
```

</details>
