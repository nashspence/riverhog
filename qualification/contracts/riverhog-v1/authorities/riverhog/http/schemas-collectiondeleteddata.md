# schemas: CollectionDeletedData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondeleteddata:e59c7a53fe -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletedData`

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
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| encoded-size | bytes | `contract_max` | maximum=4096, reason=bounded-lifecycle-event-context |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: CollectionDeletedData
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `actor` | yes | #/components/schemas/RiverhogActor |  |
| `bytes` | yes | integer |  |
| `cause` | no | object (1 fields) |  |
| `collection_created_at` | yes | string |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `context` | no | object (2 fields) |  |
| `files` | yes | integer |  |
| `initiator` | yes | #/components/schemas/RiverhogActor |  |
| `remote_storage_bytes` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c0b9a189a887a7f1070840f2395d318877c36d055780d5b36fb19f6f0411956d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "actor": {
      "$ref": "#/components/schemas/RiverhogActor"
    },
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
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
      "maxLength": 64,
      "minLength": 1,
      "title": "Collection Created At",
      "type": "string"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
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
      "minimum": 0,
      "title": "Files",
      "type": "integer"
    },
    "initiator": {
      "$ref": "#/components/schemas/RiverhogActor"
    },
    "remote_storage_bytes": {
      "minimum": 0,
      "title": "Remote Storage Bytes",
      "type": "integer"
    }
  },
  "required": [
    "actor",
    "initiator",
    "collection_id",
    "collection_created_at",
    "files",
    "bytes",
    "remote_storage_bytes"
  ],
  "title": "CollectionDeletedData",
  "type": "object"
}
```
