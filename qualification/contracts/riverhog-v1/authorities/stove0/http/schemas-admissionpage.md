# schemas: AdmissionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-admissionpage:34c761a7cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-d8099e381e9a"></a>
- <a id="s-19178e63f64f"></a>`title`: AdmissionPage
- <a id="s-6f94d51bc4f0"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d5f270254c30"></a>`admissions` | yes | type="array"; items=(#/components/schemas/AdmissionView) |  |
| <a id="s-3e881f688540"></a>`filters` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-9a36eb1fac20"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-3123ed8fc830"></a>`order` | yes | type="string"; enum=["asc","desc"] |  |
| <a id="s-d9ac4cae183b"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-36fb196aae18"></a>`policy_id` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-cbcb64b233a3"></a>`sort` | yes | type="string"; enum=["created_at","updated_at","state","admission_id"] |  |
| <a id="s-f4c994c20cde"></a>`state` | yes | anyOf=type="string"; enum=["intent","previewed","work_bound"] \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field filters](#s-3e881f688540) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field admissions](#s-d5f270254c30) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-d9ac4cae183b) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: AdmissionView](schemas-admissionview.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-e97ebdce177e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-c12f3e791e66"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-5663169207fc"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)
- <a id="pa-c486dd8d6d7f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
