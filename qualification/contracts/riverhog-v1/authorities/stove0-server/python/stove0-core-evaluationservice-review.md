# stove0_core.EvaluationService.review

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationservice-review:1732afc7fc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-517402d752"></a>
- <a id="s-6396811a5c"></a>`distribution`: `stove0-server`
- <a id="s-83f265eb46"></a>`module`: `stove0_core`
- <a id="s-fd4b0cca6d"></a>`name`: `review`
- <a id="s-e9e7a4fbd5"></a>`owner`: `stove0_core.EvaluationService`
- <a id="s-55ff472f04"></a>`unit`: `member`

### Declared structure

- <a id="s-f8eee46880"></a>`kind`: `"method"`
- <a id="s-ec8bb17e39"></a>`signature`: `"\"(self, evaluation_id: 'str', review: 'EvaluationReview') -> 'EvaluationRecord'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.EvaluationService](stove0-core-evaluationservice.md)

## Governing policies

- <a id="pa-c1a191c6c5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationService.review`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b49c5ea6b4d9370b78190a67ee872c08cb97e31c45d0a316daace2397b0b7758 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str', review: 'EvaluationReview') -> 'EvaluationRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "review",
  "owner": "stove0_core.EvaluationService",
  "unit": "member"
}
```
