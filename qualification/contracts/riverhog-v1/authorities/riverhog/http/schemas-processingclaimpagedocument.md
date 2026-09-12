# schemas: ProcessingClaimPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimpagedocument:da885eec14 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-6041073899"></a>
- <a id="s-a020b53a87"></a>`title`: ProcessingClaimPageDocument
- <a id="s-8ce43a30b1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8e545456d5"></a>`claims` | yes | type="array"; items=(#/components/schemas/ProcessingClaimDocument) |  |
| <a id="s-ef1ff79919"></a>`filters` | yes | #/components/schemas/ProcessingClaimFiltersDocument |  |
| <a id="s-7409e9be17"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-aad6de037e"></a>`order` | yes | #/components/schemas/SortOrder |  |
| <a id="s-a12a255dad"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-239812ea90"></a>`sort` | yes | #/components/schemas/ProcessingClaimSort |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claims](#s-8e545456d5) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-a12a255dad) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: ProcessingClaimDocument](schemas-processingclaimdocument.md)
- [schemas: ProcessingClaimFiltersDocument](schemas-processingclaimfiltersdocument.md)
- [schemas: ProcessingClaimSort](schemas-processingclaimsort.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-c7001c3e06"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-1f08537b87"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-950ff56f53"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimPageDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 671fc271e83cea3f708e84026473a959d1ec05965a2294f734e1b4459b4c6a42 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "claims": {
      "items": {
        "$ref": "#/components/schemas/ProcessingClaimDocument"
      },
      "title": "Claims",
      "type": "array"
    },
    "filters": {
      "$ref": "#/components/schemas/ProcessingClaimFiltersDocument"
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
    "sort": {
      "$ref": "#/components/schemas/ProcessingClaimSort"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "filters",
    "claims"
  ],
  "title": "ProcessingClaimPageDocument",
  "type": "object"
}
```
