# schemas: CollectionProvenanceVerificationJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionprovenanceverificationjobout:58529eb02d -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionProvenanceVerificationJobOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CollectionProvenanceVerificationOut](schemas-collectionprovenanceverificationout.md)

## Contract summary

- `title`: CollectionProvenanceVerificationJobOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `attempts` | yes | integer |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `failure` | yes | object (2 fields) |  |
| `finished_at` | yes | object (2 fields) |  |
| `requested_at` | yes | string |  |
| `result` | yes | object (1 fields) |  |
| `started_at` | yes | object (2 fields) |  |
| `state` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de7cbad7331cf2f94febc0c3d07c3b32e066668ffe9796a2c2c66063f8e4cfc5 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "attempts": {
      "minimum": 0,
      "title": "Attempts",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "failure": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Failure"
    },
    "finished_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Finished At"
    },
    "requested_at": {
      "title": "Requested At",
      "type": "string"
    },
    "result": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionProvenanceVerificationOut"
        },
        {
          "type": "null"
        }
      ]
    },
    "started_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Started At"
    },
    "state": {
      "enum": [
        "queued",
        "running",
        "canceling",
        "succeeded",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "state",
    "requested_at",
    "started_at",
    "finished_at",
    "attempts",
    "result",
    "failure"
  ],
  "title": "CollectionProvenanceVerificationJobOut",
  "type": "object"
}
```
