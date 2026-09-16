# stove0_operator_contracts.EvaluationCreatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationcreatedevent:234180fdf8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a9e93a2755"></a>
- <a id="s-8212001da0"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-6ce32f60bb"></a>`module`: `stove0_operator_contracts`
- <a id="s-a72cde56eb"></a>`name`: `EvaluationCreatedEvent`
- <a id="s-6311f4ff52"></a>`unit`: `export`

### Declared structure

- <a id="s-f3bb6464e6"></a>`kind`: `"class"`
- <a id="s-136e1859ad"></a>`signature`: `"\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.evaluation.created'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.EvaluationCreatedEventData) -> None\""`

#### Validated model schema

<a id="s-23ad66d4b5"></a>
- <a id="s-e3cc3f3417"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-add4371446"></a>`data` | yes | #/$defs/EvaluationCreatedEventData |  |
| <a id="s-feabbe1e8e"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-4ac098cf9a"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-8f5573b03c"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-04a29a1dbe"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-4b10b4cd7d"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-92854b5e7f"></a>`time` | yes | type="string" |  |
| <a id="s-e30bee9869"></a>`type` | yes | type="string"; const="io.riverhog.stove0.evaluation.created" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-dbda641bbc"></a>`EvaluationCreatedEventData` | type="object"; fields=`evaluation_id`, `phase`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [validate_time](stove0-operator-contracts-evaluationcreatedevent-validate-time.md)
- [exact_subject](stove0-operator-contracts-evaluationcreatedevent-exact-subject.md)

## Governing policies

- <a id="pa-f745cb5c96"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationCreatedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a789ad26acf8fe68accad5f298666239550079c1b60a6a9ac84875d08f0acc57 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "EvaluationCreatedEventData": {
          "additionalProperties": false,
          "properties": {
            "evaluation_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "phase": {
              "enum": [
                "planning",
                "running",
                "partially_complete",
                "complete",
                "failed",
                "canceled"
              ],
              "type": "string"
            }
          },
          "required": [
            "evaluation_id",
            "phase"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/EvaluationCreatedEventData"
        },
        "datacontenttype": {
          "const": "application/json",
          "default": "application/json",
          "type": "string"
        },
        "id": {
          "minLength": 1,
          "type": "string"
        },
        "source": {
          "const": "urn:riverhog:stove0",
          "type": "string"
        },
        "specversion": {
          "const": "1.0",
          "default": "1.0",
          "type": "string"
        },
        "subject": {
          "minLength": 1,
          "type": "string"
        },
        "time": {
          "type": "string"
        },
        "type": {
          "const": "io.riverhog.stove0.evaluation.created",
          "type": "string"
        }
      },
      "required": [
        "id",
        "source",
        "type",
        "subject",
        "time",
        "data"
      ],
      "type": "object"
    },
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.evaluation.created'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.EvaluationCreatedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationCreatedEvent",
  "unit": "export"
}
```
