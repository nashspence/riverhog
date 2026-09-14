# stove0_operator_contracts.SchedulerRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerrun:7c5d6153ae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9052785ed5"></a>
- <a id="s-e6d0614c12"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-3af0525ee1"></a>`module`: `stove0_operator_contracts`
- <a id="s-a21009d9bc"></a>`name`: `SchedulerRun`
- <a id="s-7d4de8abc1"></a>`unit`: `export`

### Declared structure

- <a id="s-707b5c1086"></a>`kind`: `"class"`
- <a id="s-ac3c4af9b1"></a>`signature`: `"'(*, pruning: stove0_operator_contracts.SchedulerPruning \| None, admission: stove0_operator_contracts.AdmissionRun \| None = None, work: stove0_operator_contracts.SchedulerWorkBatch) -> None'"`

#### Validated model schema

<a id="s-bea95f16ce"></a>
- <a id="s-c7db1dc77d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4529d53325"></a>`admission` | no | anyOf=#/$defs/AdmissionRun \| type="null" |  |
| <a id="s-d0ff3529cf"></a>`pruning` | yes | anyOf=#/$defs/SchedulerPruning \| type="null" |  |
| <a id="s-557354c3af"></a>`work` | yes | #/$defs/SchedulerWorkBatch |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-bba32c184c"></a>`AdmissionRun` | type="object"; fields=`failures`, `progressed`; additional keys=`additionalProperties`, `required` |
| <a id="s-f931be92db"></a>`SchedulerFailure` | type="object"; fields=`error`, `event_id`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-bc7006be46"></a>`SchedulerPruning` | type="object"; fields=`evaluation_bytes`, `evaluations`, `event_bytes`, `events`, `selection_bytes`, `selections`, `work`, `work_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-85f50ba4ce"></a>`SchedulerWorkBatch` | type="object"; fields=`cursor`, `failures`, `next_cursor`, `progressed`, `role`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-324752f5b0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerRun`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7871590768273f2167839930d35587f3bce0f42c659838b422c6e9c1cd7e06e0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "AdmissionRun": {
          "additionalProperties": false,
          "properties": {
            "failures": {
              "default": [],
              "items": {
                "$ref": "#/$defs/SchedulerFailure"
              },
              "type": "array"
            },
            "progressed": {
              "items": {
                "type": "string"
              },
              "type": "array"
            }
          },
          "required": [
            "progressed"
          ],
          "type": "object"
        },
        "SchedulerFailure": {
          "additionalProperties": false,
          "properties": {
            "error": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            "event_id": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "work_id": {
              "anyOf": [
                {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            }
          },
          "required": [
            "error"
          ],
          "type": "object"
        },
        "SchedulerPruning": {
          "additionalProperties": false,
          "properties": {
            "evaluation_bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "evaluations": {
              "minimum": 0,
              "type": "integer"
            },
            "event_bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "events": {
              "minimum": 0,
              "type": "integer"
            },
            "selection_bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "selections": {
              "minimum": 0,
              "type": "integer"
            },
            "work": {
              "minimum": 0,
              "type": "integer"
            },
            "work_bytes": {
              "minimum": 0,
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
          "type": "object"
        },
        "SchedulerWorkBatch": {
          "additionalProperties": false,
          "properties": {
            "cursor": {
              "type": "string"
            },
            "failures": {
              "items": {
                "$ref": "#/$defs/SchedulerFailure"
              },
              "type": "array"
            },
            "next_cursor": {
              "type": "string"
            },
            "progressed": {
              "items": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "type": "array"
            },
            "role": {
              "enum": [
                "controller",
                "worker",
                "combined"
              ],
              "type": "string"
            }
          },
          "required": [
            "role",
            "cursor",
            "next_cursor",
            "progressed",
            "failures"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "admission": {
          "anyOf": [
            {
              "$ref": "#/$defs/AdmissionRun"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "pruning": {
          "anyOf": [
            {
              "$ref": "#/$defs/SchedulerPruning"
            },
            {
              "type": "null"
            }
          ]
        },
        "work": {
          "$ref": "#/$defs/SchedulerWorkBatch"
        }
      },
      "required": [
        "pruning",
        "work"
      ],
      "type": "object"
    },
    "signature": "'(*, pruning: stove0_operator_contracts.SchedulerPruning | None, admission: stove0_operator_contracts.AdmissionRun | None = None, work: stove0_operator_contracts.SchedulerWorkBatch) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerRun",
  "unit": "export"
}
```
