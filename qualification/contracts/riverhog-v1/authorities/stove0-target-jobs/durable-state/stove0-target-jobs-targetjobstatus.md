# stove0-target-jobs: TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-target-jobs:stove0-target-jobs-targetjobstatus:e0ff216756 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-jobs](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-bda4c8b11d"></a>
- Document: `TargetJobStatus`

### Document schema

<a id="s-c6d4e24c5d"></a>
- <a id="s-dfc60cdd41"></a>`title`: TargetJobStatus
- <a id="s-c122f3742c"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3df251316d"></a>`attempt` | yes | type="integer"; minimum=1 |  |
| <a id="s-c6836f380a"></a>`derivation` | no | anyOf=type="object"; additional keys=`additionalProperties` \| type="null" |  |
| <a id="s-5e317243bc"></a>`effect_receipt` | no | anyOf=#/$defs/ExternalEffectReceipt \| type="null" |  |
| <a id="s-18f9ef6552"></a>`execution_evidence` | no | anyOf=#/$defs/TargetExecutionEvidence \| type="null" |  |
| <a id="s-ae3c3516eb"></a>`failure` | no | anyOf=#/$defs/TargetFailure \| type="null" |  |
| <a id="s-4637ad6e2c"></a>`inapplicable` | no | anyOf=#/$defs/TargetInapplicable \| type="null" |  |
| <a id="s-251d0845a8"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-82ccbbb62d"></a>`output_collection` | no | anyOf=#/$defs/OutputCollectionRef \| type="null" |  |
| <a id="s-30b6e78e10"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5126e9e1e1"></a>`production` | no | anyOf=#/$defs/TargetProductionAuthority \| type="null" |  |
| <a id="s-3c2424e430"></a>`progress` | yes | #/$defs/TargetProgress |  |
| <a id="s-e91fef0a8f"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"] |  |
| <a id="s-6dc483ad49"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-79937baa0d"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-94e71eb9c2"></a>`ArtifactDispositionSetIdentity` | type="object"; fields=`disposition_count`, `output_artifact_count`, `output_edge_count`, `sha256`; additional keys=`required` |
| <a id="s-090ac9d376"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-26a5e8c3b1"></a>`ExternalEffectReceipt` | type="object"; fields=`execution_sha256`, `format`, `job_id`, `operation_contract_sha256`, `plan_sha256`, `receipt_sha256`, `request_sha256`, `result`, `target_contract_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-18dd5136d0"></a>`JsonValue` | empty object |
| <a id="s-b4c30465ea"></a>`OutputArtifactRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-c8fdc5e154"></a>`OutputArtifactSetIdentity` | type="object"; fields=`artifact_count`, `roles`, `sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-d7db2264de"></a>`OutputCollectionRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `derivation_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-4cb88d14a9"></a>`TargetExecutionEvidence` | type="object"; fields=`execution_sha256`, `operation_contract_sha256`, `plan_sha256`, `runtime`, `target_contract_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-9a77996437"></a>`TargetFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-9eef827dbc"></a>`TargetInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-960e8adbc4"></a>`TargetProductionAuthority` | type="object"; fields=`disposition_count`, `disposition_sha256`, `format`, `job_id`, `outputs`, `plan_sha256`, `production_sha256`, `riverhog_disposition_set`, `source_edge_count`, `source_edge_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-80fe077bf8"></a>`TargetProgress` | type="object"; fields=`completed`, `phase`, `total`, `unit`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [Schema identity](stove0-target-jobs-durable-state-identity.md)

## Governing policies

- <a id="pa-ab85be943c"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:stove0-target-jobs](../../../evidence/sources.md#src-7b4138829a) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/protocol.py`

### Machine authority

- `/external_contract/durable_state/owners/5/structure/documents/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88691d8e5fc7229865708347d4ad12582b9eec7c0dbeb258b9d89e0bc8db1f0c -->

```json
{
  "id": "TargetJobStatus",
  "schema": {
    "$defs": {
      "ArtifactDispositionSetIdentity": {
        "description": "Small identity for one sealed claim-scoped relational disposition set.",
        "properties": {
          "disposition_count": {
            "title": "Disposition Count",
            "type": "integer"
          },
          "output_artifact_count": {
            "title": "Output Artifact Count",
            "type": "integer"
          },
          "output_edge_count": {
            "title": "Output Edge Count",
            "type": "integer"
          },
          "sha256": {
            "title": "Sha256",
            "type": "string"
          }
        },
        "required": [
          "disposition_count",
          "output_edge_count",
          "output_artifact_count",
          "sha256"
        ],
        "title": "ArtifactDispositionSetIdentity",
        "type": "object"
      },
      "CollectionId": {
        "minimum": 1,
        "type": "integer"
      },
      "ExternalEffectReceipt": {
        "additionalProperties": false,
        "properties": {
          "execution_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Execution Sha256",
            "type": "string"
          },
          "format": {
            "const": "stove0-external-effect-receipt/v1",
            "default": "stove0-external-effect-receipt/v1",
            "title": "Format",
            "type": "string"
          },
          "job_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Job Id",
            "type": "string"
          },
          "operation_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Operation Contract Sha256",
            "type": "string"
          },
          "plan_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Plan Sha256",
            "type": "string"
          },
          "receipt_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Receipt Sha256",
            "type": "string"
          },
          "request_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Request Sha256",
            "type": "string"
          },
          "result": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Result",
            "type": "object",
            "x-riverhog-encoded-bytes-max": 65536,
            "x-riverhog-extent": {
              "policy": "contract_max",
              "reason": "bounded-external-effect-receipt"
            }
          },
          "target_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Contract Sha256",
            "type": "string"
          }
        },
        "required": [
          "job_id",
          "request_sha256",
          "target_contract_sha256",
          "operation_contract_sha256",
          "plan_sha256",
          "execution_sha256",
          "result",
          "receipt_sha256"
        ],
        "title": "ExternalEffectReceipt",
        "type": "object"
      },
      "JsonValue": {},
      "OutputArtifactRoleCount": {
        "additionalProperties": false,
        "properties": {
          "count": {
            "minimum": 1,
            "title": "Count",
            "type": "integer"
          },
          "role": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Role",
            "type": "string"
          }
        },
        "required": [
          "role",
          "count"
        ],
        "title": "OutputArtifactRoleCount",
        "type": "object"
      },
      "OutputArtifactSetIdentity": {
        "additionalProperties": false,
        "description": "Small identity for target outputs already registered with Riverhog.",
        "properties": {
          "artifact_count": {
            "minimum": 1,
            "title": "Artifact Count",
            "type": "integer"
          },
          "roles": {
            "items": {
              "$ref": "#/$defs/OutputArtifactRoleCount"
            },
            "minItems": 1,
            "title": "Roles",
            "type": "array"
          },
          "sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Sha256",
            "type": "string"
          },
          "total_bytes": {
            "minimum": 0,
            "title": "Total Bytes",
            "type": "integer"
          }
        },
        "required": [
          "artifact_count",
          "total_bytes",
          "roles",
          "sha256"
        ],
        "title": "OutputArtifactSetIdentity",
        "type": "object"
      },
      "OutputCollectionRef": {
        "additionalProperties": false,
        "properties": {
          "archive_root_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Archive Root Sha256",
            "type": "string"
          },
          "collection_id": {
            "$ref": "#/$defs/CollectionId"
          },
          "content_identity": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Content Identity",
            "type": "string"
          },
          "derivation_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Derivation Sha256",
            "type": "string"
          }
        },
        "required": [
          "collection_id",
          "archive_root_sha256",
          "content_identity",
          "derivation_sha256"
        ],
        "title": "OutputCollectionRef",
        "type": "object"
      },
      "TargetExecutionEvidence": {
        "additionalProperties": false,
        "properties": {
          "execution_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Execution Sha256",
            "type": "string"
          },
          "operation_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Operation Contract Sha256",
            "type": "string"
          },
          "plan_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Plan Sha256",
            "type": "string"
          },
          "runtime": {
            "additionalProperties": {
              "$ref": "#/$defs/JsonValue"
            },
            "title": "Runtime",
            "type": "object"
          },
          "target_contract_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Target Contract Sha256",
            "type": "string"
          }
        },
        "required": [
          "target_contract_sha256",
          "operation_contract_sha256",
          "plan_sha256",
          "execution_sha256"
        ],
        "title": "TargetExecutionEvidence",
        "type": "object"
      },
      "TargetFailure": {
        "additionalProperties": false,
        "properties": {
          "code": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Code",
            "type": "string"
          },
          "message": {
            "maxLength": 1000,
            "minLength": 1,
            "title": "Message",
            "type": "string"
          },
          "retryable": {
            "title": "Retryable",
            "type": "boolean"
          }
        },
        "required": [
          "code",
          "message",
          "retryable"
        ],
        "title": "TargetFailure",
        "type": "object"
      },
      "TargetInapplicable": {
        "additionalProperties": false,
        "properties": {
          "code": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "title": "Code",
            "type": "string"
          },
          "message": {
            "maxLength": 1000,
            "minLength": 1,
            "title": "Message",
            "type": "string"
          }
        },
        "required": [
          "code",
          "message"
        ],
        "title": "TargetInapplicable",
        "type": "object"
      },
      "TargetProductionAuthority": {
        "additionalProperties": false,
        "properties": {
          "disposition_count": {
            "minimum": 1,
            "title": "Disposition Count",
            "type": "integer"
          },
          "disposition_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Disposition Sha256",
            "type": "string"
          },
          "format": {
            "const": "stove0-target-production/v1",
            "default": "stove0-target-production/v1",
            "title": "Format",
            "type": "string"
          },
          "job_id": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Job Id",
            "type": "string"
          },
          "outputs": {
            "$ref": "#/$defs/OutputArtifactSetIdentity"
          },
          "plan_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Plan Sha256",
            "type": "string"
          },
          "production_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Production Sha256",
            "type": "string"
          },
          "riverhog_disposition_set": {
            "$ref": "#/$defs/ArtifactDispositionSetIdentity"
          },
          "source_edge_count": {
            "minimum": 1,
            "title": "Source Edge Count",
            "type": "integer"
          },
          "source_edge_sha256": {
            "pattern": "^[0-9a-f]{64}$",
            "title": "Source Edge Sha256",
            "type": "string"
          }
        },
        "required": [
          "job_id",
          "plan_sha256",
          "outputs",
          "disposition_count",
          "disposition_sha256",
          "source_edge_count",
          "source_edge_sha256",
          "riverhog_disposition_set",
          "production_sha256"
        ],
        "title": "TargetProductionAuthority",
        "type": "object"
      },
      "TargetProgress": {
        "additionalProperties": false,
        "properties": {
          "completed": {
            "minimum": 0,
            "title": "Completed",
            "type": "integer"
          },
          "phase": {
            "maxLength": 120,
            "minLength": 1,
            "title": "Phase",
            "type": "string"
          },
          "total": {
            "anyOf": [
              {
                "minimum": 0,
                "type": "integer"
              },
              {
                "type": "null"
              }
            ],
            "default": null,
            "title": "Total"
          },
          "unit": {
            "anyOf": [
              {
                "maxLength": 40,
                "minLength": 1,
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "default": null,
            "title": "Unit"
          }
        },
        "required": [
          "phase",
          "completed"
        ],
        "title": "TargetProgress",
        "type": "object"
      }
    },
    "additionalProperties": false,
    "allOf": [
      {
        "else": {
          "properties": {
            "failure": {
              "type": "null"
            }
          }
        },
        "if": {
          "properties": {
            "state": {
              "const": "failed"
            }
          }
        },
        "then": {
          "properties": {
            "failure": {
              "type": "object"
            }
          },
          "required": [
            "failure"
          ]
        }
      }
    ],
    "properties": {
      "attempt": {
        "minimum": 1,
        "title": "Attempt",
        "type": "integer"
      },
      "derivation": {
        "anyOf": [
          {
            "additionalProperties": true,
            "type": "object"
          },
          {
            "type": "null"
          }
        ],
        "default": null,
        "title": "Derivation"
      },
      "effect_receipt": {
        "anyOf": [
          {
            "$ref": "#/$defs/ExternalEffectReceipt"
          },
          {
            "type": "null"
          }
        ],
        "default": null
      },
      "execution_evidence": {
        "anyOf": [
          {
            "$ref": "#/$defs/TargetExecutionEvidence"
          },
          {
            "type": "null"
          }
        ],
        "default": null
      },
      "failure": {
        "anyOf": [
          {
            "$ref": "#/$defs/TargetFailure"
          },
          {
            "type": "null"
          }
        ],
        "default": null
      },
      "inapplicable": {
        "anyOf": [
          {
            "$ref": "#/$defs/TargetInapplicable"
          },
          {
            "type": "null"
          }
        ],
        "default": null
      },
      "job_id": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Job Id",
        "type": "string"
      },
      "output_collection": {
        "anyOf": [
          {
            "$ref": "#/$defs/OutputCollectionRef"
          },
          {
            "type": "null"
          }
        ],
        "default": null
      },
      "plan_sha256": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Plan Sha256",
        "type": "string"
      },
      "production": {
        "anyOf": [
          {
            "$ref": "#/$defs/TargetProductionAuthority"
          },
          {
            "type": "null"
          }
        ],
        "default": null
      },
      "progress": {
        "$ref": "#/$defs/TargetProgress"
      },
      "protocol": {
        "default": "stove0-transform-target/v1",
        "enum": [
          "stove0-transform-target/v1",
          "stove0-effect-target/v1"
        ],
        "title": "Protocol",
        "type": "string"
      },
      "request_sha256": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Request Sha256",
        "type": "string"
      },
      "state": {
        "enum": [
          "queued",
          "running",
          "canceling",
          "interrupted",
          "inapplicable",
          "succeeded",
          "failed",
          "canceled"
        ],
        "title": "State",
        "type": "string"
      }
    },
    "required": [
      "job_id",
      "state",
      "attempt",
      "request_sha256",
      "plan_sha256",
      "progress"
    ],
    "title": "TargetJobStatus",
    "type": "object"
  }
}
```
