# schemas: ListCollectionUploadSessionsResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-listcollectionuploadsessionsresponse:b710d6cae1 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListCollectionUploadSessionsResponse`

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
- [schemas: CollectionUploadListFiltersOut](schemas-collectionuploadlistfiltersout.md)
- [schemas: CollectionUploadListItemOut](schemas-collectionuploadlistitemout.md)
- [schemas: CollectionUploadSort](schemas-collectionuploadsort.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |

## Contract summary

- `title`: ListCollectionUploadSessionsResponse
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `filters` | yes | #/components/schemas/CollectionUploadListFiltersOut |  |
| `next_page_token` | yes | object (1 fields) |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | integer |  |
| `query` | yes | object (2 fields) |  |
| `sort` | yes | #/components/schemas/CollectionUploadSort |  |
| `uploads` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d1d503b61867473e1610211e1cffb036c25172263cacad79e7ddd59e0aa8717 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "filters": {
      "$ref": "#/components/schemas/CollectionUploadListFiltersOut"
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
      "$ref": "#/components/schemas/CollectionUploadSort"
    },
    "uploads": {
      "items": {
        "$ref": "#/components/schemas/CollectionUploadListItemOut"
      },
      "title": "Uploads",
      "type": "array"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "filters",
    "uploads"
  ],
  "title": "ListCollectionUploadSessionsResponse",
  "type": "object"
}
```
