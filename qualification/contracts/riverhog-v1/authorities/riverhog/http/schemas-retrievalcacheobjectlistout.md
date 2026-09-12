# schemas: RetrievalCacheObjectListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcacheobjectlistout:576bfb05a5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-d146425e2cb6"></a>
- <a id="s-f6db17f11c7f"></a>`title`: RetrievalCacheObjectListOut
- <a id="s-f5770d06226f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-30b2a98980a8"></a>`filters` | yes | #/components/schemas/RetrievalCacheObjectListFiltersOut |  |
| <a id="s-1896763d3381"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-80a1fca6a6b1"></a>`objects` | yes | type="array"; items=(#/components/schemas/RetrievalCacheObjectOut) |  |
| <a id="s-8569d48df232"></a>`order` | yes | #/components/schemas/SortOrder |  |
| <a id="s-b5cbb0e784ee"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-e9876fe3eb12"></a>`query` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-6edb24988127"></a>`sort` | yes | #/components/schemas/RetrievalCacheSort |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field objects](#s-80a1fca6a6b1) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-b5cbb0e784ee) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: RetrievalCacheObjectListFiltersOut](schemas-retrievalcacheobjectlistfiltersout.md)
- [schemas: RetrievalCacheObjectOut](schemas-retrievalcacheobjectout.md)
- [schemas: RetrievalCacheSort](schemas-retrievalcachesort.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-eb534fde7db4"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-096451ce1ea0"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)
- <a id="pa-49d7d34b26a8"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
