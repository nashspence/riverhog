# schemas: SearchResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-searchresponse:2e182b7e0c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-e64b807a4f"></a>
- <a id="s-cf9bcc0b9d"></a>`title`: SearchResponse
- <a id="s-e2fb76d6c3"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1e9fcde2d6"></a>`collection` | yes | anyOf=#/components/schemas/CollectionId \| type="null" |  |
| <a id="s-0d7c551f3a"></a>`files` | yes | type="array"; items=(#/components/schemas/SearchFileOut) |  |
| <a id="s-a2e3054466"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-d0c8462799"></a>`order` | yes | #/components/schemas/SortOrder |  |
| <a id="s-dd8aeabd12"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-a185b44ebe"></a>`query` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-29873fe494"></a>`sort` | yes | #/components/schemas/SearchSort |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-0d7c551f3a) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-dd8aeabd12) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: SearchFileOut](schemas-searchfileout.md)
- [schemas: SearchSort](schemas-searchsort.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-5ec850f0cb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-ce07c49466"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-24384c6cba"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/SearchResponse`

### Exact owned JSON

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
