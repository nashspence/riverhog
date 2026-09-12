# schemas: RetrievalCacheObjectListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcacheobjectlistout:576bfb05a5 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheObjectListOut`

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
- [schemas: RetrievalCacheObjectListFiltersOut](schemas-retrievalcacheobjectlistfiltersout.md)
- [schemas: RetrievalCacheObjectOut](schemas-retrievalcacheobjectout.md)
- [schemas: RetrievalCacheSort](schemas-retrievalcachesort.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: RetrievalCacheObjectListOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `filters` | yes | #/components/schemas/RetrievalCacheObjectListFiltersOut |  |
| `next_page_token` | yes | object (1 fields) |  |
| `objects` | yes | array |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | integer |  |
| `query` | yes | object (2 fields) |  |
| `sort` | yes | #/components/schemas/RetrievalCacheSort |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 45d8f7507d818a2c11fc8221d49e8737803add14b6d7c3ebbc344da79fbced4a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "filters": {
      "$ref": "#/components/schemas/RetrievalCacheObjectListFiltersOut"
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
    "objects": {
      "items": {
        "$ref": "#/components/schemas/RetrievalCacheObjectOut"
      },
      "title": "Objects",
      "type": "array"
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
      "$ref": "#/components/schemas/RetrievalCacheSort"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "filters",
    "objects"
  ],
  "title": "RetrievalCacheObjectListOut",
  "type": "object"
}
```
