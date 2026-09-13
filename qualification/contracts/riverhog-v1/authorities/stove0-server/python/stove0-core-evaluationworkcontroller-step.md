# stove0_core.EvaluationWorkController.step

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationworkcontroller-step:d3827cf292 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4e5a3662b9"></a>
| Field | Shape |
|---|---|
| <a id="s-f4e0fabe22"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ce9357c07f"></a>`distribution` | "stove0-server" |
| <a id="s-ff2f6e2313"></a>`module` | "stove0_core" |
| <a id="s-aa1cfbba8d"></a>`name` | "step" |
| <a id="s-7868a7d065"></a>`owner` | "stove0_core.EvaluationWorkController" |
| <a id="s-d661caba41"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.EvaluationWorkController](stove0-core-evaluationworkcontroller.md)

## Governing policies

- <a id="pa-ebe5928c00"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EvaluationWorkController.step`

### Exact owned JSON

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
