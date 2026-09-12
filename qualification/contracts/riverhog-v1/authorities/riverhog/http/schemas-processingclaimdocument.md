# schemas: ProcessingClaimDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimdocument:a0c4a1f332 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 13 |

## External contract

<a id="s-fb2afd54ce"></a>
- <a id="s-54f210eb03"></a>`title`: ProcessingClaimDocument
- <a id="s-1964ac6806"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3b26ebd228"></a>`abandoned_at` | no | anyOf=type="string"; minLength=1; maxLength=64 \| type="null" |  |
| <a id="s-ff86aa1856"></a>`abandonment_reason` | no | anyOf=type="string"; minLength=1; maxLength=1000 \| type="null" |  |
| <a id="s-8bfc846875"></a>`consumer` | yes | #/components/schemas/ProcessingClaimConsumerDocument |  |
| <a id="s-9d922404c4"></a>`created_at` | yes | type="string"; minLength=1; maxLength=64 |  |
| <a id="s-672a29cfd8"></a>`expires_at` | yes | type="string"; minLength=1; maxLength=64 |  |
| <a id="s-32f010eb31"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-4eff6e6056"></a>`format` | yes | type="string"; const="riverhog-processing-claim/v1" |  |
| <a id="s-b60202a68b"></a>`id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dd008f3e61"></a>`inputs` | yes | #/components/schemas/ReceivingSetDocument |  |
| <a id="s-2a773e5157"></a>`outcome_settlement` | no | anyOf=#/components/schemas/ProcessingClaimOutcomeSettlementDocument \| type="null" |  |
| <a id="s-dd1d4f73c8"></a>`outcomes` | yes | #/components/schemas/OutcomeSetDocument |  |
| <a id="s-637e612ce8"></a>`output_collection_id` | no | anyOf=#/components/schemas/CollectionId \| type="null" |  |
| <a id="s-9049da54de"></a>`plan` | no | anyOf=#/components/schemas/ProcessingClaimPlanDocument \| type="null" |  |
| <a id="s-38ac0d9a68"></a>`purpose` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-7f23a91312"></a>`released_at` | no | anyOf=type="string"; minLength=1; maxLength=64 \| type="null" |  |
| <a id="s-ec795af7cb"></a>`settled_at` | no | anyOf=type="string"; minLength=1; maxLength=64 \| type="null" |  |
| <a id="s-bb2fa0d286"></a>`state` | yes | type="string"; enum=["active","settled","retiring","abandoned","released"] |  |
| <a id="s-1a8a5eaf65"></a>`updated_at` | yes | type="string"; minLength=1; maxLength=64 |  |
| <a id="s-3097610fdd"></a>`work_document` | yes | type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` |  |
| <a id="s-1c037e7801"></a>`work_document_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8fb6149ecc"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field work_document](#s-3097610fdd) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-e20c4978b2"></a>[field abandoned_at · string value](#s-3b26ebd228) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| <a id="s-335ccf771c"></a>[field abandonment_reason · string value](#s-ff86aa1856) | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| [field created_at](#s-9d922404c4) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field expires_at](#s-672a29cfd8) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field id](#s-b60202a68b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field purpose](#s-38ac0d9a68) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| <a id="s-31c5a76041"></a>[field released_at · string value](#s-7f23a91312) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| <a id="s-3585c81332"></a>[field settled_at · string value](#s-ec795af7cb) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field updated_at](#s-1a8a5eaf65) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field work_document](#s-3097610fdd) | `encoded-size · bytes · contract_max` | maximum=4194304; reason="bounded-work-document-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field work_document_sha256](#s-1c037e7801) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field work_id](#s-8fb6149ecc) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: OutcomeSetDocument](schemas-outcomesetdocument.md)
- [schemas: ProcessingClaimConsumerDocument](schemas-processingclaimconsumerdocument.md)
- [schemas: ProcessingClaimOutcomeSettlementDocument](schemas-processingclaimoutcomesettlementdocument.md)
- [schemas: ProcessingClaimPlanDocument](schemas-processingclaimplandocument.md)
- [schemas: ReceivingSetDocument](schemas-receivingsetdocument.md)

## Governing policies

- <a id="pa-3aee72ad46"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-44f2743201"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-767fa2afa6"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
