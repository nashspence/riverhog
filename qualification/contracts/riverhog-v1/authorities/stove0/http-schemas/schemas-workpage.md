# schemas: WorkPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workpage:2b85735863 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-028a221a46"></a>

- <a id="s-35787e9137"></a>`type`: `"object"`
- <a id="s-47f977eee0"></a>`additionalProperties`: `false`
- <a id="s-e0559d7e98"></a>`required`: `["page_size","next_page_token","sort","order","filters","work"]`
- <a id="s-fc938f7a66"></a>`title`: `"WorkPage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-777f0fd330"></a>`filters` | yes | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Filters" |  |
| <a id="s-6eb60aca74"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-4db74e66e7"></a>`order` | yes | type="string"; enum=["asc","desc"]; title="Order" |  |
| <a id="s-6d8c9840b3"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-80af7f878f"></a>`sort` | yes | type="string"; enum=["updated_at","phase","work_id"]; title="Sort" |  |
| <a id="s-076e711809"></a>`work` | yes | type="array"; items=([WorkView](schemas-workview.md)); title="Work" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field filters](#s-777f0fd330) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field work](#s-076e711809) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-6d8c9840b3) | `value · schema-value · contract_max` | shared above |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb).

Exact evidence groups for this contract element:

- [stove0-read-collection-progression/v1](../../../evidence/qualifications/stove0-read-collection-progression-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [BrowsePageToken](schemas-browsepagetoken.md)
- [JsonValue](schemas-jsonvalue.md)
- [WorkView](schemas-workview.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2cf0b74158"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-87aed63766"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-7b1f16c577"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-e80d2b25c9"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6370febf612ce20fe00988b97796d80b355395d5318e554b75ab17fc261c6f5 -->

```json
{
  "additionalProperties": false,
  "properties": {
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
    "sort": {
      "enum": [
        "updated_at",
        "phase",
        "work_id"
      ],
      "title": "Sort",
      "type": "string"
    },
    "work": {
      "items": {
        "$ref": "#/components/schemas/WorkView"
      },
      "title": "Work",
      "type": "array"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "filters",
    "work"
  ],
  "title": "WorkPage",
  "type": "object"
}
```

</details>
