# schemas: AdmissionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionpage:34c761a7cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: AdmissionPage
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `admissions` | yes | type="array"; items=(#/components/schemas/AdmissionView) |  |
| `filters` | yes | type="object"; additional keys=`additionalProperties` |  |
| `next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| `order` | yes | type="string"; enum=["asc","desc"] |  |
| `page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| `policy_id` | yes | anyOf=type="string" \| type="null" |  |
| `sort` | yes | type="string"; enum=["created_at","updated_at","state","admission_id"] |  |
| `state` | yes | anyOf=type="string"; enum=["intent","previewed","work_bound"] \| type="null" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: AdmissionView](schemas-admissionview.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionPage`

### Exact owned JSON

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
