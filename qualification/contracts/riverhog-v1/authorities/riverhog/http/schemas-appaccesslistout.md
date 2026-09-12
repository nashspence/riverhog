# schemas: AppAccessListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appaccesslistout:5af804bfe7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: AppAccessListOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `access` | yes | type="array"; items=(#/components/schemas/AppAccessListItemOut) |  |
| `filters` | yes | #/components/schemas/AppAccessListFiltersOut |  |
| `next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| `query` | yes | anyOf=type="string" \| type="null" |  |
| `sort` | yes | #/components/schemas/ApplicationAccessSort |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: AppAccessListFiltersOut](schemas-appaccesslistfiltersout.md)
- [schemas: AppAccessListItemOut](schemas-appaccesslistitemout.md)
- [schemas: ApplicationAccessSort](schemas-applicationaccesssort.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
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
