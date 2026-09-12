# schemas: ArchiveCopyJobListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyjoblistout:4d316d5212 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-1e640239fe57"></a>
- <a id="s-ddc136013cee"></a>`title`: ArchiveCopyJobListOut
- <a id="s-05c1c9f22397"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4db973405efd"></a>`copies` | yes | type="array"; items=(#/components/schemas/ArchiveCopyJobOut) |  |
| <a id="s-5f552953c998"></a>`filters` | yes | #/components/schemas/ArchiveCopyJobListFiltersOut |  |
| <a id="s-35da4724d81c"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-55b6ab5d2088"></a>`order` | yes | #/components/schemas/SortOrder |  |
| <a id="s-f77467ada2b5"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-5571409571ac"></a>`query` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-2815c28bbe0d"></a>`sort` | yes | #/components/schemas/ArchiveCopySort |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field copies](#s-4db973405efd) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-f77467ada2b5) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveCopyJobListFiltersOut](schemas-archivecopyjoblistfiltersout.md)
- [schemas: ArchiveCopyJobOut](schemas-archivecopyjobout.md)
- [schemas: ArchiveCopySort](schemas-archivecopysort.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-1258b7c30b0a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-54cb61fddac0"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)
- <a id="pa-3e075798d51f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobListOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30eab820038f814255f58c7bc6e17b93f72f43e4fd06287596eac6dea9ef82c1 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "copies": {
      "items": {
        "$ref": "#/components/schemas/ArchiveCopyJobOut"
      },
      "title": "Copies",
      "type": "array"
    },
    "filters": {
      "$ref": "#/components/schemas/ArchiveCopyJobListFiltersOut"
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
      "$ref": "#/components/schemas/ArchiveCopySort"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "filters",
    "copies"
  ],
  "title": "ArchiveCopyJobListOut",
  "type": "object"
}
```
