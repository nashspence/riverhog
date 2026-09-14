# stove0_operator_contracts.Stove0EventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0eventpage:953f89e83c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6cf1c87427"></a>
- <a id="s-fd58b575c8"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-f3a6e143ee"></a>`module`: `stove0_operator_contracts`
- <a id="s-e8aa9fc6f7"></a>`name`: `Stove0EventPage`
- <a id="s-5c6a732e20"></a>`unit`: `export`

### Declared structure

- <a id="s-421ddbdef2"></a>`kind`: `"class"`
- <a id="s-6b2a819afe"></a>`signature`: `"'(*, events: list[Stove0LifecycleEvent], next_cursor: str, has_more: bool) -> None'"`

#### Validated model schema

<a id="s-2ab0e0d052"></a>
- <a id="s-2f2701364f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cf3ed5a20a"></a>`events` | yes | type="array"; items=(#/$defs/Stove0LifecycleEvent) |  |
| <a id="s-f66cbd171c"></a>`has_more` | yes | type="boolean" |  |
| <a id="s-8198408c2b"></a>`next_cursor` | yes | type="string" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-696ca6dc56"></a>`BranchSetAdmittedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-e062cfc1fb"></a>`BranchSetAdmittedEventData` | type="object"; fields=`admitted_work_count`, `branch_count`, `branch_set_sha256`, `phase`, `revision`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-154fa9e25a"></a>`EvaluationCreatedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-76b044aa8b"></a>`EvaluationCreatedEventData` | type="object"; fields=`evaluation_id`, `phase`; additional keys=`additionalProperties`, `required` |
| <a id="s-839e04dd67"></a>`EvaluationUpdatedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-bf4fa5097c"></a>`EvaluationUpdatedEventData` | type="object"; fields=`evaluation_id`, `phase`, `revision`; additional keys=`additionalProperties`, `required` |
| <a id="s-ddb7cc84c4"></a>`JoinAdmittedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-4d75b42669"></a>`JoinAdmittedEventData` | type="object"; fields=`branch_set_sha256`, `join_plan_sha256`, `join_work_id`, `phase`, `revision`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-65476282a3"></a>`Stove0LifecycleEvent` | oneOf=#/$defs/WorkCreatedEvent \| #/$defs/WorkUpdatedEvent \| #/$defs/BranchSetAdmittedEvent \| #/$defs/JoinAdmittedEvent \| #/$defs/EvaluationCreatedEvent \| #/$defs/EvaluationUpdatedEvent; additional keys=`discriminator` |
| <a id="s-5eaf67dd9d"></a>`WorkCreatedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-e484369c30"></a>`WorkCreatedEventData` | type="object"; fields=`branch_set_sha256`, `join_plan_sha256`, `parent_work_id`, `phase`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-dfaa67187b"></a>`WorkUpdatedEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |
| <a id="s-d34baf0de6"></a>`WorkUpdatedEventData` | type="object"; fields=`phase`, `revision`, `work_id`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.Stove0EventPage.require_progress_after](stove0-operator-contracts-stove0eventpage-require-progress-after.md)

## Governing policies

- <a id="pa-2b9942b22c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0EventPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a166d7747cc25a404cfc9dd9df76af4fb0cc90e6efcbf0ec023fe1299867953c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "BranchSetAdmittedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/BranchSetAdmittedEventData"
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
              "const": "io.riverhog.stove0.branch-set.admitted",
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
        "BranchSetAdmittedEventData": {
          "additionalProperties": false,
          "properties": {
            "admitted_work_count": {
              "minimum": 1,
              "type": "integer"
            },
            "branch_count": {
              "minimum": 1,
              "type": "integer"
            },
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "phase": {
              "const": "coordinating",
              "type": "string"
            },
            "revision": {
              "minimum": 2,
              "type": "integer"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "work_id",
            "phase",
            "revision",
            "branch_set_sha256",
            "branch_count",
            "admitted_work_count"
          ],
          "type": "object"
        },
        "EvaluationCreatedEvent": {
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
        },
        "EvaluationUpdatedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/EvaluationUpdatedEventData"
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
              "const": "io.riverhog.stove0.evaluation.updated",
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
        "EvaluationUpdatedEventData": {
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
            },
            "revision": {
              "minimum": 2,
              "type": "integer"
            }
          },
          "required": [
            "evaluation_id",
            "phase",
            "revision"
          ],
          "type": "object"
        },
        "JoinAdmittedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/JoinAdmittedEventData"
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
              "const": "io.riverhog.stove0.join.admitted",
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
        "JoinAdmittedEventData": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "join_plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "join_work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "phase": {
              "const": "coordinating",
              "type": "string"
            },
            "revision": {
              "minimum": 2,
              "type": "integer"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "work_id",
            "phase",
            "revision",
            "branch_set_sha256",
            "join_plan_sha256",
            "join_work_id"
          ],
          "type": "object"
        },
        "Stove0LifecycleEvent": {
          "discriminator": {
            "mapping": {
              "io.riverhog.stove0.branch-set.admitted": "#/$defs/BranchSetAdmittedEvent",
              "io.riverhog.stove0.evaluation.created": "#/$defs/EvaluationCreatedEvent",
              "io.riverhog.stove0.evaluation.updated": "#/$defs/EvaluationUpdatedEvent",
              "io.riverhog.stove0.join.admitted": "#/$defs/JoinAdmittedEvent",
              "io.riverhog.stove0.work.created": "#/$defs/WorkCreatedEvent",
              "io.riverhog.stove0.work.updated": "#/$defs/WorkUpdatedEvent"
            },
            "propertyName": "type"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/WorkCreatedEvent"
            },
            {
              "$ref": "#/$defs/WorkUpdatedEvent"
            },
            {
              "$ref": "#/$defs/BranchSetAdmittedEvent"
            },
            {
              "$ref": "#/$defs/JoinAdmittedEvent"
            },
            {
              "$ref": "#/$defs/EvaluationCreatedEvent"
            },
            {
              "$ref": "#/$defs/EvaluationUpdatedEvent"
            }
          ]
        },
        "WorkCreatedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/WorkCreatedEventData"
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
              "const": "io.riverhog.stove0.work.created",
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
        "WorkCreatedEventData": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
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
            },
            "join_plan_sha256": {
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
            },
            "parent_work_id": {
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
            },
            "phase": {
              "enum": [
                "eligible",
                "claimed",
                "observing",
                "planning",
                "target_preflight",
                "queued",
                "executing",
                "output_finalizing",
                "verifying",
                "settled",
                "retirement_pending",
                "coordinating",
                "abandon_pending",
                "complete",
                "inapplicable",
                "failed",
                "canceled"
              ],
              "type": "string"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "work_id",
            "phase"
          ],
          "type": "object"
        },
        "WorkUpdatedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/WorkUpdatedEventData"
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
              "const": "io.riverhog.stove0.work.updated",
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
        "WorkUpdatedEventData": {
          "additionalProperties": false,
          "properties": {
            "phase": {
              "enum": [
                "eligible",
                "claimed",
                "observing",
                "planning",
                "target_preflight",
                "queued",
                "executing",
                "output_finalizing",
                "verifying",
                "settled",
                "retirement_pending",
                "coordinating",
                "abandon_pending",
                "complete",
                "inapplicable",
                "failed",
                "canceled"
              ],
              "type": "string"
            },
            "revision": {
              "minimum": 2,
              "type": "integer"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "work_id",
            "phase",
            "revision"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "events": {
          "items": {
            "$ref": "#/$defs/Stove0LifecycleEvent"
          },
          "type": "array"
        },
        "has_more": {
          "type": "boolean"
        },
        "next_cursor": {
          "type": "string"
        }
      },
      "required": [
        "events",
        "next_cursor",
        "has_more"
      ],
      "type": "object"
    },
    "signature": "'(*, events: list[Stove0LifecycleEvent], next_cursor: str, has_more: bool) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "Stove0EventPage",
  "unit": "export"
}
```
