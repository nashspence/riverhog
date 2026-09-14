# stove0_core.EvaluationRecord.evaluation_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationrecord-evaluation-id:8ca06899ea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6856a20d3c"></a>
- <a id="s-6186014097"></a>`distribution`: `stove0-server`
- <a id="s-f947b4be3e"></a>`module`: `stove0_core`
- <a id="s-b434125a6b"></a>`name`: `evaluation_id`
- <a id="s-55151f796e"></a>`owner`: `stove0_core.EvaluationRecord`
- <a id="s-6c33b3fb0c"></a>`unit`: `member`

### Declared structure

- <a id="s-ca8c819286"></a>`kind`: `"property"`
- <a id="s-9454b8e4f1"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [EvaluationRecord](stove0-core-evaluationrecord.md)

## Governing policies

- <a id="pa-774bd26f90"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationRecord.evaluation_id`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3bda518891f7d8d303e3dfd26afdec98db98931b44c1e99d23b1d4fe6f0ff3f6 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "evaluation_id",
  "owner": "stove0_core.EvaluationRecord",
  "unit": "member"
}
```
