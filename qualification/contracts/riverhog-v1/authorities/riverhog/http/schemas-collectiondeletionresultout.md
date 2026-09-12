# schemas: CollectionDeletionResultOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondeletionresultout:abf4078ec6 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletionResultOut`

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

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: CollectionDeletionResultOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `bytes` | yes | integer |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `files` | yes | integer |  |
| `remote_storage_bytes` | yes | integer |  |
| `status` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c29c7974ad1cb9ee7c6d98e1114fd78f7388b444c98a4d910d5a816982a83be -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "title": "Bytes",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "files": {
      "title": "Files",
      "type": "integer"
    },
    "remote_storage_bytes": {
      "title": "Remote Storage Bytes",
      "type": "integer"
    },
    "status": {
      "enum": [
        "deleting",
        "deleted",
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
    "remote_storage_bytes"
  ],
  "title": "CollectionDeletionResultOut",
  "type": "object"
}
```
