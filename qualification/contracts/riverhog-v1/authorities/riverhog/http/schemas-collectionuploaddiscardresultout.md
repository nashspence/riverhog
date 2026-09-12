# schemas: CollectionUploadDiscardResultOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploaddiscardresultout:ba77563b66 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadDiscardResultOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CompleteCollectionUploadCustodyOut](schemas-completecollectionuploadcustodyout.md)
- [schemas: PendingCollectionUploadCustodyOut](schemas-pendingcollectionuploadcustodyout.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: CollectionUploadDiscardResultOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_objects` | yes | integer |  |
| `bytes` | yes | integer |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `custody` | yes | object (3 fields) |  |
| `files` | yes | integer |  |
| `status` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77320f97a51d078ec34eacf125437bd66112bf2e14d41b8ff7250a805c1a269b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_objects": {
      "title": "Archive Objects",
      "type": "integer"
    },
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "custody": {
      "discriminator": {
        "mapping": {
          "complete": "#/components/schemas/CompleteCollectionUploadCustodyOut",
          "pending": "#/components/schemas/PendingCollectionUploadCustodyOut"
        },
        "propertyName": "state"
      },
      "oneOf": [
        {
          "$ref": "#/components/schemas/PendingCollectionUploadCustodyOut"
        },
        {
          "$ref": "#/components/schemas/CompleteCollectionUploadCustodyOut"
        }
      ],
      "title": "Custody"
    },
    "files": {
      "minimum": 0,
      "title": "Files",
      "type": "integer"
    },
    "status": {
      "enum": [
        "discarded",
        "already_absent"
      ],
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "status",
    "collection_id",
    "files",
    "bytes",
    "custody",
    "archive_objects"
  ],
  "title": "CollectionUploadDiscardResultOut",
  "type": "object"
}
```
