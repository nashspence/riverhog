# schemas: RetrievalRequestedData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalrequesteddata:f7183a1e56 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalRequestedData`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: RiverhogActor](schemas-riverhogactor.md)
- [schemas: RiverhogEventCause](schemas-riverhogeventcause.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| encoded-size | bytes | `contract_max` | maximum=4096, reason=bounded-lifecycle-event-context |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=300, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: RetrievalRequestedData
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `actor` | yes | #/components/schemas/RiverhogActor |  |
| `cause` | no | object (1 fields) |  |
| `collection_created_at` | no | object (2 fields) |  |
| `collection_id` | no | object (1 fields) |  |
| `collection_ids` | yes | array |  |
| `context` | no | object (2 fields) |  |
| `files` | yes | integer |  |
| `initiator` | yes | #/components/schemas/RiverhogActor |  |
| `objects` | yes | integer |  |
| `restore_required` | yes | boolean |  |
| `retrieval_id` | yes | string |  |
| `state` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4581c86e40bd15f1d047659781e5bcabea559157f2b8989541ecad1dfb97da63 -->

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
    "files": {
      "minimum": 1,
      "title": "Files",
      "type": "integer"
    },
    "initiator": {
      "$ref": "#/components/schemas/RiverhogActor"
    },
    "objects": {
      "minimum": 1,
      "title": "Objects",
      "type": "integer"
    },
    "restore_required": {
      "title": "Restore Required",
      "type": "boolean"
    },
    "retrieval_id": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Retrieval Id",
      "type": "string"
    },
    "state": {
      "enum": [
        "requested",
        "ready"
      ],
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
    "files",
    "objects",
    "restore_required"
  ],
  "title": "RetrievalRequestedData",
  "type": "object"
}
```
