# schemas: AdmissionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionpage:34c761a7cb -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionPage`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: AdmissionView](schemas-admissionview.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: AdmissionPage
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `admissions` | yes | array |  |
| `filters` | yes | object |  |
| `next_page_token` | yes | object (1 fields) |  |
| `order` | yes | string |  |
| `page_size` | yes | integer |  |
| `policy_id` | yes | object (2 fields) |  |
| `sort` | yes | string |  |
| `state` | yes | object (2 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 64c354d167a868fd912f6e49e4b65f39165b214ae9d619e5381d7286c259a40f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "admissions": {
      "items": {
        "$ref": "#/components/schemas/AdmissionView"
      },
      "title": "Admissions",
      "type": "array"
    },
    "filters": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Filters",
      "type": "object"
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
      "enum": [
        "asc",
        "desc"
      ],
      "title": "Order",
      "type": "string"
    },
    "page_size": {
      "maximum": 100,
      "minimum": 1,
      "title": "Page Size",
      "type": "integer"
    },
    "policy_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Policy Id"
    },
    "sort": {
      "enum": [
        "created_at",
        "updated_at",
        "state",
        "admission_id"
      ],
      "title": "Sort",
      "type": "string"
    },
    "state": {
      "anyOf": [
        {
          "enum": [
            "intent",
            "previewed",
            "work_bound"
          ],
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "State"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "filters",
    "policy_id",
    "state",
    "admissions"
  ],
  "title": "AdmissionPage",
  "type": "object"
}
```
