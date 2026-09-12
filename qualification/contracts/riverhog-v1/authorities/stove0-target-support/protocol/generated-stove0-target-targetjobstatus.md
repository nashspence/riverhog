# generated:stove0-target: TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-targetjobstatus:b1d1aa5cfc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-target-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

- `title`: TargetJobStatus
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `attempt` | yes | type="integer"; minimum=1 |  |
| `derivation` | no | anyOf=type="object"; additional keys=`additionalProperties` \| type="null" |  |
| `effect_receipt` | no | anyOf=#/$defs/ExternalEffectReceipt \| type="null" |  |
| `execution_evidence` | no | anyOf=#/$defs/TargetExecutionEvidence \| type="null" |  |
| `failure` | no | anyOf=#/$defs/TargetFailure \| type="null" |  |
| `inapplicable` | no | anyOf=#/$defs/TargetInapplicable \| type="null" |  |
| `job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `output_collection` | no | anyOf=#/$defs/OutputCollectionRef \| type="null" |  |
| `plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `production` | no | anyOf=#/$defs/TargetProductionAuthority \| type="null" |  |
| `progress` | yes | #/$defs/TargetProgress |  |
| `protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"] |  |
| `request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"] |  |

### Definitions

| Definition | Shape |
|---|---|
| `ArtifactDispositionSetIdentity` | type="object"; fields=`disposition_count`, `output_artifact_count`, `output_edge_count`, `sha256`; additional keys=`required` |
| `CollectionId` | type="integer"; minimum=1 |
| `ExternalEffectReceipt` | type="object"; fields=`execution_sha256`, `format`, `job_id`, `operation_contract_sha256`, `plan_sha256`, `receipt_sha256`, `request_sha256`, `result`, `target_contract_sha256`; additional keys=`additionalProperties`, `required` |
| `JsonValue` | empty object |
| `OutputArtifactRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |
| `OutputArtifactSetIdentity` | type="object"; fields=`artifact_count`, `roles`, `sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| `OutputCollectionRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `derivation_sha256`; additional keys=`additionalProperties`, `required` |
| `TargetExecutionEvidence` | type="object"; fields=`execution_sha256`, `operation_contract_sha256`, `plan_sha256`, `runtime`, `target_contract_sha256`; additional keys=`additionalProperties`, `required` |
| `TargetFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| `TargetInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| `TargetProductionAuthority` | type="object"; fields=`disposition_count`, `disposition_sha256`, `format`, `job_id`, `outputs`, `plan_sha256`, `production_sha256`, `riverhog_disposition_set`, `source_edge_count`, `source_edge_sha256`; additional keys=`additionalProperties`, `required` |
| `TargetProgress` | type="object"; fields=`completed`, `phase`, `total`, `unit`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:stove0-target` — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetJobStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c29a6125c1975f597c8de13fed65f3aa4e4286f4616fa74161074c7217e67684 -->

```json
{
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
```
