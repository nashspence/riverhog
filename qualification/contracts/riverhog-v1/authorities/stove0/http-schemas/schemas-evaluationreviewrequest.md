# schemas: EvaluationReviewRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-evaluationreviewrequest:f0311a1310 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-ed8f2572e0"></a>

- <a id="s-8201af224f"></a>`type`: `"object"`
- <a id="s-e1dbd70928"></a>`additionalProperties`: `false`
- <a id="s-7fd0672159"></a>`title`: `"EvaluationReviewRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1d61a58396"></a>`note` | no | anyOf=[(type="string"; maxLength=4000; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$"); (type="null")]; title="Note" |  |
| <a id="s-861148bbb1"></a>`rating` | no | anyOf=[(type="integer"; minimum=1; maximum=5); (type="null")]; title="Rating" |  |

### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `anyOf` alternative 1](#s-984bc4ba60) |
| 2 | [See `anyOf` alternative 2](#s-e20819a9c8) |

### <a id="s-984bc4ba60"></a>`anyOf` alternative 1

- <a id="s-6b38f49419"></a>`required`: `["rating"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1b21858451"></a>`rating` | yes | type="integer" |  |

### <a id="s-e20819a9c8"></a>`anyOf` alternative 2

- <a id="s-4667ba0439"></a>`required`: `["note"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6c388424d2"></a>`note` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-09e7971127"></a>[field note · string value](#s-1d61a58396) | `length · characters · contract_max` | maximum=4000 |
| <a id="s-7f7f403d37"></a>[field rating · integer value](#s-861148bbb1) | `value · schema-value · contract_max` | maximum=5 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f7ae926d32"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-0e19fc8cd6"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationReviewRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2160c5bc0466346c7fbd2f3b813e9086de3169e440e4e98e2f5a6ab52df07b76 -->

```json
{
  "additionalProperties": false,
  "anyOf": [
    {
      "properties": {
        "rating": {
          "type": "integer"
        }
      },
      "required": [
        "rating"
      ]
    },
    {
      "properties": {
        "note": {
          "type": "string"
        }
      },
      "required": [
        "note"
      ]
    }
  ],
  "properties": {
    "note": {
      "anyOf": [
        {
          "maxLength": 4000,
          "minLength": 1,
          "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Note"
    },
    "rating": {
      "anyOf": [
        {
          "maximum": 5,
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Rating"
    }
  },
  "title": "EvaluationReviewRequest",
  "type": "object"
}
```

</details>
