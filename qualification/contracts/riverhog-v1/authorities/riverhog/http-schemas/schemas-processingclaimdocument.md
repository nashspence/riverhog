# schemas: ProcessingClaimDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingclaimdocument:15c5f076e7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-fb2afd54ce"></a>

- <a id="s-1964ac6806"></a>`type`: `"object"`
- <a id="s-006574c524"></a>`additionalProperties`: `false`
- <a id="s-0981842cab"></a>`required`: `["format","id","work_id","consumer","purpose","state","fence","expires_at","created_at","updated_at","work_document","work_document_sha256","inputs","outcomes"]`
- <a id="s-54f210eb03"></a>`title`: `"ProcessingClaimDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3b26ebd228"></a>`abandoned_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; title="Abandoned At" |  |
| <a id="s-ff86aa1856"></a>`abandonment_reason` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; title="Abandonment Reason" |  |
| <a id="s-8bfc846875"></a>`consumer` | yes | [ProcessingClaimConsumerDocument](schemas-processingclaimconsumerdocument.md) |  |
| <a id="s-9d922404c4"></a>`created_at` | yes | type="string"; maxLength=64; minLength=1; title="Created At" |  |
| <a id="s-672a29cfd8"></a>`expires_at` | yes | type="string"; maxLength=64; minLength=1; title="Expires At" |  |
| <a id="s-32f010eb31"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-4eff6e6056"></a>`format` | yes | type="string"; const="riverhog-processing-claim/v1"; title="Format" |  |
| <a id="s-b60202a68b"></a>`id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Id" |  |
| <a id="s-dd008f3e61"></a>`inputs` | yes | [ReceivingSetDocument](schemas-receivingsetdocument.md) |  |
| <a id="s-2a773e5157"></a>`outcome_settlement` | no | anyOf=[([ProcessingClaimOutcomeSettlementDocument](schemas-processingclaimoutcomesettlementdocument.md)); (type="null")] |  |
| <a id="s-dd1d4f73c8"></a>`outcomes` | yes | [OutcomeSetDocument](schemas-outcomesetdocument.md) |  |
| <a id="s-637e612ce8"></a>`output_collection_id` | no | anyOf=[([CollectionId](schemas-collectionid.md)); (type="null")] |  |
| <a id="s-9049da54de"></a>`plan` | no | anyOf=[([ProcessingClaimPlanDocument](schemas-processingclaimplandocument.md)); (type="null")] |  |
| <a id="s-38ac0d9a68"></a>`purpose` | yes | type="string"; maxLength=160; minLength=1; title="Purpose" |  |
| <a id="s-7f23a91312"></a>`released_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; title="Released At" |  |
| <a id="s-ec795af7cb"></a>`settled_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; title="Settled At" |  |
| <a id="s-bb2fa0d286"></a>`state` | yes | type="string"; enum=["active","settled","retiring","abandoned","released"]; title="State" |  |
| <a id="s-1a8a5eaf65"></a>`updated_at` | yes | type="string"; maxLength=64; minLength=1; title="Updated At" |  |
| <a id="s-3097610fdd"></a>`work_document` | yes | type="object"; additionalProperties=(any JSON value); title="Work Document"; x-riverhog-encoded-bytes-max=4194304; x-riverhog-extent={"policy":"contract_max","reason":"bounded-work-document-envelope"} |  |
| <a id="s-1c037e7801"></a>`work_document_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Document Sha256" |  |
| <a id="s-8fb6149ecc"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-334e821425"></a>1 | properties={state: (enum=["settled","retiring","released"])} | properties={settled_at: (type="string")}; required=["settled_at"] | properties={settled_at: (type="null")} |
| <a id="s-3d91042db2"></a>2 | properties={state: (const="abandoned")} | properties={abandoned_at: (type="string"); abandonment_reason: (type="string")}; required=["abandoned_at","abandonment_reason"] | properties={abandoned_at: (type="null"); abandonment_reason: (type="null")} |
| <a id="s-3f25a72f11"></a>3 | properties={state: (const="released")} | properties={released_at: (type="string")}; required=["released_at"] | properties={released_at: (type="null")} |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field work_document](#s-3097610fdd) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

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

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [OutcomeSetDocument](schemas-outcomesetdocument.md)
- [ProcessingClaimConsumerDocument](schemas-processingclaimconsumerdocument.md)
- [ProcessingClaimOutcomeSettlementDocument](schemas-processingclaimoutcomesettlementdocument.md)
- [ProcessingClaimPlanDocument](schemas-processingclaimplandocument.md)
- [ReceivingSetDocument](schemas-receivingsetdocument.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-323180bdb7"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-4137fe7856"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-b0c685b7ed"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
