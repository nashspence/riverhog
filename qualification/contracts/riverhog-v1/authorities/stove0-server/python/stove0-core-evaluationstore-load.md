# stove0_core.EvaluationStore.load

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationstore-load:8edb0a05ec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-522996edd3"></a>
- <a id="s-18beeef881"></a>`distribution`: `stove0-server`
- <a id="s-b02bdb63bb"></a>`module`: `stove0_core`
- <a id="s-20f12cdc4c"></a>`name`: `load`
- <a id="s-b66050c59f"></a>`owner`: `stove0_core.EvaluationStore`
- <a id="s-05beb8b88d"></a>`unit`: `member`

### Declared structure

- <a id="s-de87cd150a"></a>`kind`: `"method"`
- <a id="s-616a536b37"></a>`signature`: `"\"(self, evaluation_id: 'str') -> 'EvaluationRecord \| None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.EvaluationStore](stove0-core-evaluationstore.md)

## Governing policies

- <a id="pa-50f2fd7f57"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationStore.load`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0b400dcc1d3cea6070aecccb21fd3498a5e5bd021fd37cdb45fcbf3341b6e16a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str') -> 'EvaluationRecord | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load",
  "owner": "stove0_core.EvaluationStore",
  "unit": "member"
}
```
