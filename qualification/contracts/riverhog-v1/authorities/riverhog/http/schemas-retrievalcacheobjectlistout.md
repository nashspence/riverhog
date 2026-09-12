# schemas: RetrievalCacheObjectListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcacheobjectlistout:576bfb05a5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: RetrievalCacheObjectListOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `filters` | yes | #/components/schemas/RetrievalCacheObjectListFiltersOut |  |
| `next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| `objects` | yes | type="array"; items=(#/components/schemas/RetrievalCacheObjectOut) |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| `query` | yes | anyOf=type="string" \| type="null" |  |
| `sort` | yes | #/components/schemas/RetrievalCacheSort |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: RetrievalCacheObjectListFiltersOut](schemas-retrievalcacheobjectlistfiltersout.md)
- [schemas: RetrievalCacheObjectOut](schemas-retrievalcacheobjectout.md)
- [schemas: RetrievalCacheSort](schemas-retrievalcachesort.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheObjectListOut`

### Exact owned JSON

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
