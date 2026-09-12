# schemas: TagSummaryOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-tagsummaryout:d965985567 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/TagSummaryOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionTag](schemas-collectiontag.md)

## Contract summary

- `title`: TagSummaryOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection_count` | yes | integer |  |
| `tag` | yes | #/components/schemas/CollectionTag |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 354b8d7177581c064ad240b844abe463b451ddae7b4de49cf6616aec2584009a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_count": {
      "minimum": 1,
      "title": "Collection Count",
      "type": "integer"
    },
    "tag": {
      "$ref": "#/components/schemas/CollectionTag"
    }
  },
  "required": [
    "tag",
    "collection_count"
  ],
  "title": "TagSummaryOut",
  "type": "object"
}
```
