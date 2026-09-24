# schemas: EvaluationPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-evaluationpage:f472187880 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f8832b7945"></a>

- <a id="s-bf521c0f24"></a>`type`: `"object"`
- <a id="s-719ecda30d"></a>`additionalProperties`: `false`
- <a id="s-81456c11b4"></a>`required`: `["page_size","next_page_token","sort","order","filters","evaluations"]`
- <a id="s-affd60995e"></a>`title`: `"EvaluationPage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d5faabd944"></a>`evaluations` | yes | type="array"; items=([EvaluationView](schemas-evaluationview.md)); title="Evaluations" |  |
| <a id="s-de52137029"></a>`filters` | yes | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Filters" |  |
| <a id="s-cc896697a8"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-30a4e0c811"></a>`order` | yes | type="string"; enum=["asc","desc"]; title="Order" |  |
| <a id="s-195de04014"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-3950ed2d14"></a>`sort` | yes | type="string"; enum=["updated_at","phase","evaluation_id"]; title="Sort" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field filters](#s-de52137029) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field evaluations](#s-d5faabd944) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-195de04014) | `value · schema-value · contract_max` | shared above |

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
- [EvaluationView](schemas-evaluationview.md)
- [JsonValue](schemas-jsonvalue.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8e29672bc9"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-485d2952a5"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-05a281c567"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-3698449ebe"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cee0d603fb538ed2c5a3cbf91eac88d0608b0b76d76e243725d7daca7f568a9d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "evaluations": {
      "items": {
        "$ref": "#/components/schemas/EvaluationView"
      },
      "title": "Evaluations",
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
    "sort": {
      "enum": [
        "updated_at",
        "phase",
        "evaluation_id"
      ],
      "title": "Sort",
      "type": "string"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "filters",
    "evaluations"
  ],
  "title": "EvaluationPage",
  "type": "object"
}
```

</details>
