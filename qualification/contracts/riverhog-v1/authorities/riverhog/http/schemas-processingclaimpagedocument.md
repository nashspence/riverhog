# schemas: ProcessingClaimPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimpagedocument:da885eec14 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimPageDocument`

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
- [schemas: ProcessingClaimDocument](schemas-processingclaimdocument.md)
- [schemas: ProcessingClaimFiltersDocument](schemas-processingclaimfiltersdocument.md)
- [schemas: ProcessingClaimSort](schemas-processingclaimsort.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: ProcessingClaimPageDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `claims` | yes | array |  |
| `filters` | yes | #/components/schemas/ProcessingClaimFiltersDocument |  |
| `next_page_token` | yes | object (1 fields) |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | integer |  |
| `sort` | yes | #/components/schemas/ProcessingClaimSort |  |

## Complete owned contract

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
