# stove0_operator_contracts.SchedulerPruning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerpruning:e65e25c26a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c550b1ab90"></a>
- <a id="s-b864aac556"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-f8517daba3"></a>`module`: `stove0_operator_contracts`
- <a id="s-c3fa11ca62"></a>`name`: `SchedulerPruning`
- <a id="s-a8376fc91d"></a>`unit`: `export`

### Declared structure

- <a id="s-41ac795e9f"></a>`kind`: `"class"`
- <a id="s-2f8744bcdc"></a>`signature`: `"'(*, work: Annotated[int, Ge(ge=0)], work_bytes: Annotated[int, Ge(ge=0)], evaluations: Annotated[int, Ge(ge=0)], evaluation_bytes: Annotated[int, Ge(ge=0)], selections: Annotated[int, Ge(ge=0)], selection_bytes: Annotated[int, Ge(ge=0)], events: Annotated[int, Ge(ge=0)], event_bytes: Annotated[int, Ge(ge=0)]) -> None'"`

#### Validated model schema

<a id="s-2c6cccb853"></a>
- <a id="s-738f9cfff5"></a>`title`: SchedulerPruning
- <a id="s-ca46b77744"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4ac34dd18f"></a>`evaluation_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-da2457bcd6"></a>`evaluations` | yes | type="integer"; minimum=0 |  |
| <a id="s-13340efda2"></a>`event_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-d174c11067"></a>`events` | yes | type="integer"; minimum=0 |  |
| <a id="s-ec7be4f946"></a>`selection_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-c80453fc9c"></a>`selections` | yes | type="integer"; minimum=0 |  |
| <a id="s-b06e45685c"></a>`work` | yes | type="integer"; minimum=0 |  |
| <a id="s-987035cd40"></a>`work_bytes` | yes | type="integer"; minimum=0 |  |

## Governing policies

- <a id="pa-56a4c0121a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerPruning`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e20ba9807979d5fbc2fd2dbf60215ee3436d9191d3a0cac5c5cf9b0a58e14246 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "evaluation_bytes": {
          "minimum": 0,
          "title": "Evaluation Bytes",
          "type": "integer"
        },
        "evaluations": {
          "minimum": 0,
          "title": "Evaluations",
          "type": "integer"
        },
        "event_bytes": {
          "minimum": 0,
          "title": "Event Bytes",
          "type": "integer"
        },
        "events": {
          "minimum": 0,
          "title": "Events",
          "type": "integer"
        },
        "selection_bytes": {
          "minimum": 0,
          "title": "Selection Bytes",
          "type": "integer"
        },
        "selections": {
          "minimum": 0,
          "title": "Selections",
          "type": "integer"
        },
        "work": {
          "minimum": 0,
          "title": "Work",
          "type": "integer"
        },
        "work_bytes": {
          "minimum": 0,
          "title": "Work Bytes",
          "type": "integer"
        }
      },
      "required": [
        "work",
        "work_bytes",
        "evaluations",
        "evaluation_bytes",
        "selections",
        "selection_bytes",
        "events",
        "event_bytes"
      ],
      "title": "SchedulerPruning",
      "type": "object"
    },
    "signature": "'(*, work: Annotated[int, Ge(ge=0)], work_bytes: Annotated[int, Ge(ge=0)], evaluations: Annotated[int, Ge(ge=0)], evaluation_bytes: Annotated[int, Ge(ge=0)], selections: Annotated[int, Ge(ge=0)], selection_bytes: Annotated[int, Ge(ge=0)], events: Annotated[int, Ge(ge=0)], event_bytes: Annotated[int, Ge(ge=0)]) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerPruning",
  "unit": "export"
}
```
