# schemas: SearchResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-searchresponse:2e182b7e0c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/SearchResponse`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: SearchFileOut](schemas-searchfileout.md)
- [schemas: SearchSort](schemas-searchsort.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: SearchResponse
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection` | yes | object (1 fields) |  |
| `files` | yes | array |  |
| `next_page_token` | yes | object (1 fields) |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | integer |  |
| `query` | yes | object (2 fields) |  |
| `sort` | yes | #/components/schemas/SearchSort |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ed630bdc4c9235091cc489b393e79151caf1d0a557362e967e5e4a202eb75ff -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionId"
        },
        {
          "type": "null"
        }
      ]
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/SearchFileOut"
      },
      "title": "Files",
      "type": "array"
    },
    "next_page_token": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/BrowsePageToken"
        },
        {
          "type": "null"
        }
      ]
    },
    "order": {
      "$ref": "#/components/schemas/SortOrder"
    },
    "page_size": {
      "maximum": 100,
      "minimum": 1,
      "title": "Page Size",
      "type": "integer"
    },
    "query": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Query"
    },
    "sort": {
      "$ref": "#/components/schemas/SearchSort"
    }
  },
  "required": [
    "query",
    "collection",
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "files"
  ],
  "title": "SearchResponse",
  "type": "object"
}
```
