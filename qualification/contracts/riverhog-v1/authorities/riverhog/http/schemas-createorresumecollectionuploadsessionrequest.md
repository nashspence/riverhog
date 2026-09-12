# schemas: CreateOrResumeCollectionUploadSessionRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-createorresumecollectionuploadsessionrequest:b45d70029f -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CreateOrResumeCollectionUploadSessionRequest`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionDescription](schemas-collectiondescription.md)
- [schemas: CollectionTag](schemas-collectiontag.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| encoded-size | bytes | `contract_max` | maximum=4096, reason=bounded-lifecycle-event-context |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=200, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `segmented_no_total_max` | maximum=100, minimum=None, reason=bounded-upload-staging-step; collection-tag-set-is-unbounded |

## Contract summary

- `title`: CreateOrResumeCollectionUploadSessionRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_store` | no | object (1 fields) |  |
| `custody_mode` | no | string |  |
| `description` | no | object (1 fields) |  |
| `event_context` | no | object (2 fields) |  |
| `idempotency_key` | yes | string |  |
| `ingest_source` | no | object (2 fields) |  |
| `initial_tag_set_identity` | yes | string |  |
| `provenance_mode` | no | string |  |
| `provenance_omission_reason` | no | object (2 fields) |  |
| `tags` | no | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc6291eecb861dbb701cf14027b2438c68f9e11cbe218c22b48f32eec8c478a8 -->

```json
{
  "additionalProperties": false,
  "oneOf": [
    {
      "properties": {
        "provenance_mode": {
          "const": "captured"
        },
        "provenance_omission_reason": {
          "type": "null"
        }
      }
    },
    {
      "properties": {
        "provenance_mode": {
          "const": "omitted"
        },
        "provenance_omission_reason": {
          "type": "string"
        }
      },
      "required": [
        "provenance_mode",
        "provenance_omission_reason"
      ]
    }
  ],
  "properties": {
    "archive_store": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArchiveStoreName"
        },
        {
          "type": "null"
        }
      ]
    },
    "custody_mode": {
      "default": "producer-retained",
      "enum": [
        "producer-retained",
        "custody-transfer"
      ],
      "title": "Custody Mode",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionDescription"
        },
        {
          "type": "null"
        }
      ]
    },
    "event_context": {
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
      "title": "Event Context"
    },
    "idempotency_key": {
      "maxLength": 200,
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Idempotency Key",
      "type": "string"
    },
    "ingest_source": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Ingest Source"
    },
    "initial_tag_set_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Initial Tag Set Identity",
      "type": "string"
    },
    "provenance_mode": {
      "default": "captured",
      "enum": [
        "captured",
        "omitted"
      ],
      "title": "Provenance Mode",
      "type": "string"
    },
    "provenance_omission_reason": {
      "anyOf": [
        {
          "minLength": 1,
          "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Provenance Omission Reason"
    },
    "tags": {
      "items": {
        "$ref": "#/components/schemas/CollectionTag"
      },
      "maxItems": 100,
      "title": "Tags",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "repeat-request",
        "reason": "bounded-upload-staging-step; collection-tag-set-is-unbounded"
      }
    }
  },
  "required": [
    "idempotency_key",
    "initial_tag_set_identity"
  ],
  "title": "CreateOrResumeCollectionUploadSessionRequest",
  "type": "object"
}
```
