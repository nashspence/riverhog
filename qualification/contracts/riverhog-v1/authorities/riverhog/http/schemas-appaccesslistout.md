# schemas: AppAccessListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appaccesslistout:5af804bfe7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-dd90b8f9fa"></a>
- <a id="s-8f19d8bf61"></a>`title`: AppAccessListOut
- <a id="s-9594c65264"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-227c4d3cb0"></a>`access` | yes | type="array"; items=(#/components/schemas/AppAccessListItemOut) |  |
| <a id="s-3a7cadd946"></a>`filters` | yes | #/components/schemas/AppAccessListFiltersOut |  |
| <a id="s-b1450b3d35"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-aa5e6807e0"></a>`order` | yes | #/components/schemas/SortOrder |  |
| <a id="s-5dabba4ac6"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-d9fbae8b32"></a>`query` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-49383a1598"></a>`sort` | yes | #/components/schemas/ApplicationAccessSort |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field access](#s-227c4d3cb0) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-5dabba4ac6) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: AppAccessListFiltersOut](schemas-appaccesslistfiltersout.md)
- [schemas: AppAccessListItemOut](schemas-appaccesslistitemout.md)
- [schemas: ApplicationAccessSort](schemas-applicationaccesssort.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-d7a5c53072"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-3d56d93480"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-a6fc96c852"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppAccessListOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eed99be8aec86b48b54b097a19b86b07ecd1fe6ef541f25d951fd430a636493b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "access": {
      "items": {
        "$ref": "#/components/schemas/AppAccessListItemOut"
      },
      "title": "Access",
      "type": "array"
    },
    "filters": {
      "$ref": "#/components/schemas/AppAccessListFiltersOut"
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
      "$ref": "#/components/schemas/ApplicationAccessSort"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "filters",
    "access"
  ],
  "title": "AppAccessListOut",
  "type": "object"
}
```
