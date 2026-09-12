# schemas: TagListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-taglistout:cc5f58742c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/TagListOut`

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
- [schemas: TagSummaryOut](schemas-tagsummaryout.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |

## Contract summary

- `title`: TagListOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `next_page_token` | yes | object (1 fields) |  |
| `page_size` | yes | integer |  |
| `query` | yes | object (2 fields) |  |
| `tags` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 939d1f07d67acf99140fd82462c289bbe3473988518f11d27aee4746fa9e9c43 -->

```json
{
  "additionalProperties": false,
  "properties": {
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
    "tags": {
      "items": {
        "$ref": "#/components/schemas/TagSummaryOut"
      },
      "title": "Tags",
      "type": "array"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "query",
    "tags"
  ],
  "title": "TagListOut",
  "type": "object"
}
```
