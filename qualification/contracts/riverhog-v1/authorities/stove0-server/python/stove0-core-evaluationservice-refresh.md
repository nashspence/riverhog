# stove0_core.EvaluationService.refresh

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationservice-refresh:a97fe4f778 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2d773b9ef4"></a>
- <a id="s-3dcd708682"></a>`distribution`: `stove0-server`
- <a id="s-f96d57d1e9"></a>`module`: `stove0_core`
- <a id="s-6cdbf4c19d"></a>`name`: `refresh`
- <a id="s-2b1caef6f2"></a>`owner`: `stove0_core.EvaluationService`
- <a id="s-5743fc85a9"></a>`unit`: `member`

### Declared structure

- <a id="s-e4ed392ac9"></a>`kind`: `"method"`
- <a id="s-6d1a317bea"></a>`signature`: `"\"(self, evaluation_id: 'str') -> 'EvaluationRecord'\""`

## Maintained corroboration

### Related interface records

- [EvaluationService](stove0-core-evaluationservice.md)

## Governing policies

- <a id="pa-48d9d5a39d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationService.refresh`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1aeea7e8440d805b543ceb3396a5e215bc49017a527f1e296e4bb66d4b402264 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str') -> 'EvaluationRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "refresh",
  "owner": "stove0_core.EvaluationService",
  "unit": "member"
}
```

</details>
