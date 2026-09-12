# schemas: AppListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-applistout:361fbcfac3 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppListOut`

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

- [schemas: AppSummaryOut](schemas-appsummaryout.md)
- [schemas: ApplicationSort](schemas-applicationsort.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: AppListOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `active` | yes | object (2 fields) |  |
| `apps` | yes | array |  |
| `next_page_token` | yes | object (1 fields) |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | integer |  |
| `query` | yes | object (2 fields) |  |
| `sort` | yes | #/components/schemas/ApplicationSort |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 01cda1da0017f7b7f54d609ad06860f21cfb161831426349b29dff3b3910d59d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "active": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "title": "Active"
    },
    "apps": {
      "items": {
        "$ref": "#/components/schemas/AppSummaryOut"
      },
      "title": "Apps",
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
      "$ref": "#/components/schemas/ApplicationSort"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "active",
    "apps"
  ],
  "title": "AppListOut",
  "type": "object"
}
```
