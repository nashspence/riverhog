# schemas: TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetjobstatus:5145d7a958 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
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
| `effect_receipt` | no | anyOf=#/components/schemas/ExternalEffectReceipt \| type="null" |  |
| `execution_evidence` | no | anyOf=#/components/schemas/TargetExecutionEvidence \| type="null" |  |
| `failure` | no | anyOf=#/components/schemas/TargetFailure \| type="null" |  |
| `inapplicable` | no | anyOf=#/components/schemas/TargetInapplicable \| type="null" |  |
| `job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `output_collection` | no | anyOf=#/components/schemas/OutputCollectionRef \| type="null" |  |
| `plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `production` | no | anyOf=#/components/schemas/TargetProductionAuthority \| type="null" |  |
| `progress` | yes | #/components/schemas/TargetProgress |  |
| `protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"] |  |
| `request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"] |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ExternalEffectReceipt](schemas-externaleffectreceipt.md)
- [schemas: OutputCollectionRef](schemas-outputcollectionref.md)
- [schemas: TargetExecutionEvidence](schemas-targetexecutionevidence.md)
- [schemas: TargetFailure](schemas-targetfailure.md)
- [schemas: TargetInapplicable](schemas-targetinapplicable.md)
- [schemas: TargetProductionAuthority](schemas-targetproductionauthority.md)
- [schemas: TargetProgress](schemas-targetprogress.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetJobStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 24d2a389a999b1cc5fbabff12afdfded3bdda88b0515f472b543541ce57f3087 -->

```json
{
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
      "title": "Derivation"
    },
    "effect_receipt": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ExternalEffectReceipt"
        },
        {
          "type": "null"
        }
      ]
    },
    "execution_evidence": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/TargetExecutionEvidence"
        },
        {
          "type": "null"
        }
      ]
    },
    "failure": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/TargetFailure"
        },
        {
          "type": "null"
        }
      ]
    },
    "inapplicable": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/TargetInapplicable"
        },
        {
          "type": "null"
        }
      ]
    },
    "job_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Job Id",
      "type": "string"
    },
    "output_collection": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/OutputCollectionRef"
        },
        {
          "type": "null"
        }
      ]
    },
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "production": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/TargetProductionAuthority"
        },
        {
          "type": "null"
        }
      ]
    },
    "progress": {
      "$ref": "#/components/schemas/TargetProgress"
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
