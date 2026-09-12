# schemas: CollectionProvenanceVerificationJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionprovenanceverificationjobout:58529eb02d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: CollectionProvenanceVerificationJobOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `attempts` | yes | type="integer"; minimum=0 |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `failure` | yes | anyOf=type="string" \| type="null" |  |
| `finished_at` | yes | anyOf=type="string" \| type="null" |  |
| `requested_at` | yes | type="string" |  |
| `result` | yes | anyOf=#/components/schemas/CollectionProvenanceVerificationOut \| type="null" |  |
| `started_at` | yes | anyOf=type="string" \| type="null" |  |
| `state` | yes | type="string"; enum=["queued","running","canceling","succeeded","failed","canceled"] |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CollectionProvenanceVerificationOut](schemas-collectionprovenanceverificationout.md)

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionProvenanceVerificationJobOut`

### Exact owned JSON

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
