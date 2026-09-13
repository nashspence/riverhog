# stove0_core.EvaluationWorkController.retry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationworkcontroller-retry:b1d8298b16 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b50c354db5"></a>
| Field | Shape |
|---|---|
| <a id="s-b72250a42d"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a4d9d1a8f4"></a>`distribution` | "stove0-server" |
| <a id="s-63919bfe8a"></a>`module` | "stove0_core" |
| <a id="s-5397a51861"></a>`name` | "retry" |
| <a id="s-b63a488234"></a>`owner` | "stove0_core.EvaluationWorkController" |
| <a id="s-ea65317fc1"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.EvaluationWorkController](stove0-core-evaluationworkcontroller.md)

## Governing policies

- <a id="pa-95aea13298"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationWorkController.retry`

### Exact owned JSON

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
