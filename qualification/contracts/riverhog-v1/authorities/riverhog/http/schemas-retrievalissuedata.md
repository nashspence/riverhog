# schemas: RetrievalIssueData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalissuedata:c618e5398d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 6 |

## External contract

<a id="s-0684cc4629"></a>
- <a id="s-805afcc7b3"></a>`title`: RetrievalIssueData
- <a id="s-2b50be487f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e41e5e822e"></a>`actor` | yes | #/components/schemas/RiverhogActor |  |
| <a id="s-cacf608ce3"></a>`cause` | no | anyOf=#/components/schemas/RiverhogEventCause \| type="null" |  |
| <a id="s-4c4268ef6a"></a>`collection_created_at` | no | anyOf=type="string"; minLength=1; maxLength=64 \| type="null" |  |
| <a id="s-dc91964e2c"></a>`collection_id` | no | anyOf=#/components/schemas/CollectionId \| type="null" |  |
| <a id="s-1ea7f25788"></a>`collection_ids` | yes | type="array"; minItems=1; items=(#/components/schemas/CollectionId) |  |
| <a id="s-dd874d7287"></a>`context` | no | anyOf=type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` \| type="null" |  |
| <a id="s-99f1d4d657"></a>`error` | yes | type="string"; minLength=1; maxLength=16384 |  |
| <a id="s-61cd3658eb"></a>`initiator` | yes | #/components/schemas/RiverhogActor |  |
| <a id="s-642cd7a6f7"></a>`retrieval_id` | yes | type="string"; minLength=1; maxLength=300 |  |
| <a id="s-f79b5c54a8"></a>`state` | yes | type="string"; const="requested" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collection_ids](#s-1ea7f25788) | `cardinality · items · operational_policy` | shared above |
| <a id="s-68023b2f77"></a>[field context · object value](#s-dd874d7287) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-7747973112"></a>[field collection_created_at · string value](#s-4c4268ef6a) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field context · object value](#s-68023b2f77) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field error](#s-99f1d4d657) | `length · characters · contract_max` | maximum=16384; minimum=1; reason="schema-maximum" |
| [field retrieval_id](#s-642cd7a6f7) | `length · characters · contract_max` | maximum=300; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: RiverhogActor](schemas-riverhogactor.md)
- [schemas: RiverhogEventCause](schemas-riverhogeventcause.md)

## Governing policies

- <a id="pa-819b95c55b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-06e98472c4"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-ea6526e9da"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalIssueData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e149aec6574de95dcad648d4f68006f0d27b1c3f5728be4e18857378b36852ed -->

```json
{
  "additionalProperties": false,
  "properties": {
    "actor": {
      "$ref": "#/components/schemas/RiverhogActor"
    },
    "cause": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/RiverhogEventCause"
        },
        {
          "type": "null"
        }
      ]
    },
    "collection_created_at": {
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
      "title": "Collection Created At"
    },
    "collection_id": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionId"
        },
        {
          "type": "null"
        }
      ]
    },
    "collection_ids": {
      "items": {
        "$ref": "#/components/schemas/CollectionId"
      },
      "minItems": 1,
      "title": "Collection Ids",
      "type": "array"
    },
    "context": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 4096,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-lifecycle-event-context"
          }
        },
        {
          "type": "null"
        }
      ],
      "title": "Context"
    },
    "error": {
      "maxLength": 16384,
      "minLength": 1,
      "title": "Error",
      "type": "string"
    },
    "initiator": {
      "$ref": "#/components/schemas/RiverhogActor"
    },
    "retrieval_id": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Retrieval Id",
      "type": "string"
    },
    "state": {
      "const": "requested",
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "actor",
    "initiator",
    "retrieval_id",
    "collection_ids",
    "state",
    "error"
  ],
  "title": "RetrievalIssueData",
  "type": "object"
}
```
