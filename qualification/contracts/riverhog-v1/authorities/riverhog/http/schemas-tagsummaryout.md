# schemas: TagSummaryOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-tagsummaryout:d965985567 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: TagSummaryOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection_count` | yes | type="integer"; minimum=1 |  |
| `tag` | yes | #/components/schemas/CollectionTag |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionTag](schemas-collectiontag.md)

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

- `/external_contract/http_openapi/riverhog/components/schemas/TagSummaryOut`

### Exact owned JSON

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
