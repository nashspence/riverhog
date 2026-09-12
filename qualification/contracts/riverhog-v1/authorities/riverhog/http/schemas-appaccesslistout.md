# schemas: AppAccessListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appaccesslistout:5af804bfe7 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppAccessListOut`

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

- [schemas: AppAccessListFiltersOut](schemas-appaccesslistfiltersout.md)
- [schemas: AppAccessListItemOut](schemas-appaccesslistitemout.md)
- [schemas: ApplicationAccessSort](schemas-applicationaccesssort.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: AppAccessListOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `access` | yes | array |  |
| `filters` | yes | #/components/schemas/AppAccessListFiltersOut |  |
| `next_page_token` | yes | object (1 fields) |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | integer |  |
| `query` | yes | object (2 fields) |  |
| `sort` | yes | #/components/schemas/ApplicationAccessSort |  |

## Complete owned contract

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
