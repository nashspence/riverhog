# schemas: AppListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-applistout:3dd0d5347c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-80f6c42475"></a>

- <a id="s-4fe169554c"></a>`type`: `"object"`
- <a id="s-d3612bc3dc"></a>`additionalProperties`: `false`
- <a id="s-8af3ed08da"></a>`required`: `["page_size","next_page_token","sort","order","query","active","apps"]`
- <a id="s-a9f201503c"></a>`title`: `"AppListOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0ea75186b8"></a>`active` | yes | anyOf=[(type="boolean"); (type="null")]; title="Active" |  |
| <a id="s-3150fae7ba"></a>`apps` | yes | type="array"; items=([AppSummaryOut](schemas-appsummaryout.md)); title="Apps" |  |
| <a id="s-32d47ef58a"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-185f629968"></a>`order` | yes | [SortOrder](schemas-sortorder.md) |  |
| <a id="s-ba79b45d65"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-0dab9e8171"></a>`query` | yes | anyOf=[(type="string"); (type="null")]; title="Query" |  |
| <a id="s-99665b4ee9"></a>`sort` | yes | [ApplicationSort](schemas-applicationsort.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field apps](#s-3150fae7ba) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-ba79b45d65) | `value · schema-value · contract_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-read-collection-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-1536c4a29a)

## Maintained corroboration

### Referenced contract dossiers

- [AppSummaryOut](schemas-appsummaryout.md)
- [ApplicationSort](schemas-applicationsort.md)
- [BrowsePageToken](schemas-browsepagetoken.md)
- [SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-1946fd969b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-60b3a6bad4"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-7aa50fcc91"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppListOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
