# schemas: ListCollectionUploadSessionsResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-listcollectionuploadsessionsresponse:b710d6cae1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-cb3d2c41a9"></a>
- <a id="s-74d14fa1d0"></a>`title`: ListCollectionUploadSessionsResponse
- <a id="s-f3d9d94506"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d2ea9c3cc3"></a>`filters` | yes | #/components/schemas/CollectionUploadListFiltersOut |  |
| <a id="s-849c5a09ca"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-3c8c0c69e9"></a>`order` | yes | #/components/schemas/SortOrder |  |
| <a id="s-5f8d2ec911"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-80d2dee606"></a>`query` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-61648fd94b"></a>`sort` | yes | #/components/schemas/CollectionUploadSort |  |
| <a id="s-b2370a9c91"></a>`uploads` | yes | type="array"; items=(#/components/schemas/CollectionUploadListItemOut) |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field uploads](#s-b2370a9c91) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-5f8d2ec911) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CollectionUploadListFiltersOut](schemas-collectionuploadlistfiltersout.md)
- [schemas: CollectionUploadListItemOut](schemas-collectionuploadlistitemout.md)
- [schemas: CollectionUploadSort](schemas-collectionuploadsort.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-1145f1a762"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-86b6d70e11"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-195056b0e3"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListCollectionUploadSessionsResponse`

### Exact owned JSON

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
