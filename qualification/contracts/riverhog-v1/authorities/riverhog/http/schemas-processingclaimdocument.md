# schemas: ProcessingClaimDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimdocument:a0c4a1f332 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 13 |

## External contract

- `title`: ProcessingClaimDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `abandoned_at` | no | anyOf=type="string"; minLength=1; maxLength=64 \| type="null" |  |
| `abandonment_reason` | no | anyOf=type="string"; minLength=1; maxLength=1000 \| type="null" |  |
| `consumer` | yes | #/components/schemas/ProcessingClaimConsumerDocument |  |
| `created_at` | yes | type="string"; minLength=1; maxLength=64 |  |
| `expires_at` | yes | type="string"; minLength=1; maxLength=64 |  |
| `fence` | yes | type="integer"; minimum=1 |  |
| `format` | yes | type="string"; const="riverhog-processing-claim/v1" |  |
| `id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `inputs` | yes | #/components/schemas/ReceivingSetDocument |  |
| `outcome_settlement` | no | anyOf=#/components/schemas/ProcessingClaimOutcomeSettlementDocument \| type="null" |  |
| `outcomes` | yes | #/components/schemas/OutcomeSetDocument |  |
| `output_collection_id` | no | anyOf=#/components/schemas/CollectionId \| type="null" |  |
| `plan` | no | anyOf=#/components/schemas/ProcessingClaimPlanDocument \| type="null" |  |
| `purpose` | yes | type="string"; minLength=1; maxLength=160 |  |
| `released_at` | no | anyOf=type="string"; minLength=1; maxLength=64 \| type="null" |  |
| `settled_at` | no | anyOf=type="string"; minLength=1; maxLength=64 \| type="null" |  |
| `state` | yes | type="string"; enum=["active","settled","retiring","abandoned","released"] |  |
| `updated_at` | yes | type="string"; minLength=1; maxLength=64 |  |
| `work_document` | yes | type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` |  |
| `work_document_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| encoded-size | bytes | `contract_max` | maximum=4194304, reason=bounded-work-document-envelope |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: OutcomeSetDocument](schemas-outcomesetdocument.md)
- [schemas: ProcessingClaimConsumerDocument](schemas-processingclaimconsumerdocument.md)
- [schemas: ProcessingClaimOutcomeSettlementDocument](schemas-processingclaimoutcomesettlementdocument.md)
- [schemas: ProcessingClaimPlanDocument](schemas-processingclaimplandocument.md)
- [schemas: ReceivingSetDocument](schemas-receivingsetdocument.md)

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
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c659a523c955b07ee9225a1168e104782d855dd98607cf1648705c659a69381 -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "else": {
        "properties": {
          "settled_at": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "enum": [
              "settled",
              "retiring",
              "released"
            ]
          }
        }
      },
      "then": {
        "properties": {
          "settled_at": {
            "type": "string"
          }
        },
        "required": [
          "settled_at"
        ]
      }
    },
    {
      "else": {
        "properties": {
          "abandoned_at": {
            "type": "null"
          },
          "abandonment_reason": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "const": "abandoned"
          }
        }
      },
      "then": {
        "properties": {
          "abandoned_at": {
            "type": "string"
          },
          "abandonment_reason": {
            "type": "string"
          }
        },
        "required": [
          "abandoned_at",
          "abandonment_reason"
        ]
      }
    },
    {
      "else": {
        "properties": {
          "released_at": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "const": "released"
          }
        }
      },
      "then": {
        "properties": {
          "released_at": {
            "type": "string"
          }
        },
        "required": [
          "released_at"
        ]
      }
    }
  ],
  "properties": {
    "abandoned_at": {
      "anyOf": [
        {
          "maxLength": 64,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Abandoned At"
    },
    "abandonment_reason": {
      "anyOf": [
        {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Abandonment Reason"
    },
    "consumer": {
      "$ref": "#/components/schemas/ProcessingClaimConsumerDocument"
    },
    "created_at": {
      "maxLength": 64,
      "minLength": 1,
      "title": "Created At",
      "type": "string"
    },
    "expires_at": {
      "maxLength": 64,
      "minLength": 1,
      "title": "Expires At",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "format": {
      "const": "riverhog-processing-claim/v1",
      "title": "Format",
      "type": "string"
    },
    "id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Id",
      "type": "string"
    },
    "inputs": {
      "$ref": "#/components/schemas/ReceivingSetDocument"
    },
    "outcome_settlement": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ProcessingClaimOutcomeSettlementDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "outcomes": {
      "$ref": "#/components/schemas/OutcomeSetDocument"
    },
    "output_collection_id": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionId"
        },
        {
          "type": "null"
        }
      ]
    },
    "plan": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ProcessingClaimPlanDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "purpose": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Purpose",
      "type": "string"
    },
    "released_at": {
      "anyOf": [
        {
          "maxLength": 64,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Released At"
    },
    "settled_at": {
      "anyOf": [
        {
          "maxLength": 64,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Settled At"
    },
    "state": {
      "enum": [
        "active",
        "settled",
        "retiring",
        "abandoned",
        "released"
      ],
      "title": "State",
      "type": "string"
    },
    "updated_at": {
      "maxLength": 64,
      "minLength": 1,
      "title": "Updated At",
      "type": "string"
    },
    "work_document": {
      "additionalProperties": true,
      "title": "Work Document",
      "type": "object",
      "x-riverhog-encoded-bytes-max": 4194304,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-work-document-envelope"
      }
    },
    "work_document_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Document Sha256",
      "type": "string"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "format",
    "id",
    "work_id",
    "consumer",
    "purpose",
    "state",
    "fence",
    "expires_at",
    "created_at",
    "updated_at",
    "work_document",
    "work_document_sha256",
    "inputs",
    "outcomes"
  ],
  "title": "ProcessingClaimDocument",
  "type": "object"
}
```
