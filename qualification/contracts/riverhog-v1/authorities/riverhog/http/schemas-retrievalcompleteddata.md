# schemas: RetrievalCompletedData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcompleteddata:47694b6183 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-056ed2dfde"></a>
- <a id="s-61ce9d0757"></a>`title`: RetrievalCompletedData
- <a id="s-120c6934ea"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-75c41a2961"></a>`actor` | yes | #/components/schemas/RiverhogActor |  |
| <a id="s-1cdc5b55de"></a>`cause` | no | anyOf=#/components/schemas/RiverhogEventCause \| type="null" |  |
| <a id="s-a2fc38c4b7"></a>`collection_created_at` | no | anyOf=type="string"; minLength=1; maxLength=64 \| type="null" |  |
| <a id="s-c1f47b1801"></a>`collection_id` | no | anyOf=#/components/schemas/CollectionId \| type="null" |  |
| <a id="s-68847b14ff"></a>`collection_ids` | yes | type="array"; minItems=1; items=(#/components/schemas/CollectionId) |  |
| <a id="s-b7bbd451e9"></a>`context` | no | anyOf=type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` \| type="null" |  |
| <a id="s-49e730c378"></a>`initiator` | yes | #/components/schemas/RiverhogActor |  |
| <a id="s-faa54f4904"></a>`retrieval_id` | yes | type="string"; minLength=1; maxLength=300 |  |
| <a id="s-fc7a4df698"></a>`state` | yes | type="string"; const="completed" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collection_ids](#s-68847b14ff) | `cardinality · items · operational_policy` | shared above |
| <a id="s-890ad10e53"></a>[field context · object value](#s-b7bbd451e9) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-650fc1ef83"></a>[field collection_created_at · string value](#s-a2fc38c4b7) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field context · object value](#s-890ad10e53) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field retrieval_id](#s-faa54f4904) | `length · characters · contract_max` | maximum=300; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: RiverhogActor](schemas-riverhogactor.md)
- [schemas: RiverhogEventCause](schemas-riverhogeventcause.md)

## Governing policies

- <a id="pa-d990dd39bc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-3be742a625"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-2be219a329"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCompletedData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 83508e13c2e95546866cfb2f88ba2d2bbbf011f5d9b047bdc0f66fdc5c6fb395 -->

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
      "const": "completed",
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "actor",
    "initiator",
    "retrieval_id",
    "collection_ids",
    "state"
  ],
  "title": "RetrievalCompletedData",
  "type": "object"
}
```
