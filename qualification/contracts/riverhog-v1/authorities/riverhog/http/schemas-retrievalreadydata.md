# schemas: RetrievalReadyData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalreadydata:6014a30a5d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 6 |

## External contract

<a id="s-b925d87b1a"></a>
- <a id="s-efa13cc032"></a>`title`: RetrievalReadyData
- <a id="s-ab5e8d56e3"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7e2852ec59"></a>`actor` | yes | #/components/schemas/RiverhogActor |  |
| <a id="s-9afac4e20b"></a>`cause` | no | anyOf=#/components/schemas/RiverhogEventCause \| type="null" |  |
| <a id="s-d9a2b57fb8"></a>`collection_created_at` | no | anyOf=type="string"; minLength=1; maxLength=64 \| type="null" |  |
| <a id="s-328580e7cb"></a>`collection_id` | no | anyOf=#/components/schemas/CollectionId \| type="null" |  |
| <a id="s-59cca2fa8c"></a>`collection_ids` | yes | type="array"; minItems=1; items=(#/components/schemas/CollectionId) |  |
| <a id="s-e36e872632"></a>`context` | no | anyOf=type="object"; additional keys=`additionalProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` \| type="null" |  |
| <a id="s-c9b96c35d6"></a>`expires_at` | yes | type="string"; minLength=1; maxLength=64 |  |
| <a id="s-a0510bc5ae"></a>`initiator` | yes | #/components/schemas/RiverhogActor |  |
| <a id="s-74697df86a"></a>`retrieval_id` | yes | type="string"; minLength=1; maxLength=300 |  |
| <a id="s-69b850b257"></a>`state` | yes | type="string"; const="ready" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collection_ids](#s-59cca2fa8c) | `cardinality · items · operational_policy` | shared above |
| <a id="s-39a0f4caa6"></a>[field context · object value](#s-e36e872632) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-234357e1a5"></a>[field collection_created_at · string value](#s-d9a2b57fb8) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field context · object value](#s-39a0f4caa6) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field expires_at](#s-c9b96c35d6) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field retrieval_id](#s-74697df86a) | `length · characters · contract_max` | maximum=300; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: RiverhogActor](schemas-riverhogactor.md)
- [schemas: RiverhogEventCause](schemas-riverhogeventcause.md)

## Governing policies

- <a id="pa-312ab8dcd2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-764f94f838"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-2e3c56bf4d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalReadyData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac9216466b428610bb8faef838ef0299950015db70aa3cbe29638afb1366e75b -->

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
    "expires_at": {
      "maxLength": 64,
      "minLength": 1,
      "title": "Expires At",
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
      "const": "ready",
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
    "expires_at"
  ],
  "title": "RetrievalReadyData",
  "type": "object"
}
```
