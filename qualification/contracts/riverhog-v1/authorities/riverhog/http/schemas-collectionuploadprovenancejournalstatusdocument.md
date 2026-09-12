# schemas: CollectionUploadProvenanceJournalStatusDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadprovenancejournal-ba2d3d9a98:4790d84516 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadProvenanceJournalStatusDocument`

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

- [schemas: ProvenanceJournalId](schemas-provenancejournalid.md)
- [schemas: ProvenanceStateId](schemas-provenancestateid.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: CollectionUploadProvenanceJournalStatusDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `accepted_bytes` | yes | integer |  |
| `bytes` | yes | integer |  |
| `current_bytes` | no | object (2 fields) |  |
| `current_path` | no | object (2 fields) |  |
| `current_sha256` | no | object (2 fields) |  |
| `current_state_id` | no | object (1 fields) |  |
| `failure` | no | object (2 fields) |  |
| `journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| `sha256` | yes | string |  |
| `state` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c7fc6e75fbd9fde5ed7dcae5ff41845ece61b2feec55575dc2a37b709bdc269 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "accepted_bytes": {
      "minimum": 0,
      "title": "Accepted Bytes",
      "type": "integer"
    },
    "bytes": {
      "minimum": 1,
      "title": "Bytes",
      "type": "integer"
    },
    "current_bytes": {
      "anyOf": [
        {
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Current Bytes"
    },
    "current_path": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Current Path"
    },
    "current_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Current Sha256"
    },
    "current_state_id": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ProvenanceStateId"
        },
        {
          "type": "null"
        }
      ]
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
    "journal_id": {
      "$ref": "#/components/schemas/ProvenanceJournalId"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    },
    "state": {
      "enum": [
        "accepting",
        "validating",
        "sealed",
        "failed"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "journal_id",
    "state",
    "bytes",
    "sha256",
    "accepted_bytes"
  ],
  "title": "CollectionUploadProvenanceJournalStatusDocument",
  "type": "object"
}
```
