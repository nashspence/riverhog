# schemas: AdmissionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-admissionpage:f1f9f03a11 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d8099e381e"></a>

- <a id="s-6f94d51bc4"></a>`type`: `"object"`
- <a id="s-60b56522e4"></a>`additionalProperties`: `false`
- <a id="s-a364b74bbf"></a>`required`: `["page_size","next_page_token","sort","order","filters","policy_id","state","admissions"]`
- <a id="s-19178e63f6"></a>`title`: `"AdmissionPage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d5f270254c"></a>`admissions` | yes | type="array"; items=([AdmissionView](schemas-admissionview.md)); title="Admissions" |  |
| <a id="s-3e881f6885"></a>`filters` | yes | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Filters" |  |
| <a id="s-9a36eb1fac"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-3123ed8fc8"></a>`order` | yes | type="string"; enum=["asc","desc"]; title="Order" |  |
| <a id="s-d9ac4cae18"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-36fb196aae"></a>`policy_id` | yes | anyOf=[(type="string"); (type="null")]; title="Policy Id" |  |
| <a id="s-cbcb64b233"></a>`sort` | yes | type="string"; enum=["created_at","updated_at","state","admission_id"]; title="Sort" |  |
| <a id="s-f4c994c20c"></a>`state` | yes | anyOf=[(type="string"; enum=["intent","previewed","work_bound"]); (type="null")]; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field filters](#s-3e881f6885) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field admissions](#s-d5f270254c) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-d9ac4cae18) | `value · schema-value · contract_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [stove0-read-collection-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-34931f753b)

## Maintained corroboration

### Referenced contract dossiers

- [AdmissionView](schemas-admissionview.md)
- [BrowsePageToken](schemas-browsepagetoken.md)
- [JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-36141ecd4d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-830ace549a"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-4acb5f91bc"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-99662a1113"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/AdmissionPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
