# schemas: CollectionUploadTagsOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadtagsout:afafee60df -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: CollectionUploadTagsOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `added` | yes | type="integer"; minimum=0 |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `tag_count` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)

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

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadTagsOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c2e980a84f0c1aca7b9eec8edfe249d27310773f6adbb3c8db687924abf9196 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "added": {
      "minimum": 0,
      "title": "Added",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "tag_count": {
      "minimum": 0,
      "title": "Tag Count",
      "type": "integer"
    }
  },
  "required": [
    "collection_id",
    "added",
    "tag_count"
  ],
  "title": "CollectionUploadTagsOut",
  "type": "object"
}
```
