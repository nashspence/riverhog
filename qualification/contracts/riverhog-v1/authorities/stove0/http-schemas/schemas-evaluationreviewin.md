# schemas: EvaluationReviewIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-evaluationreviewin:fb8f5090ee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d1a5ea0009"></a>

- <a id="s-68c174083d"></a>`type`: `"object"`
- <a id="s-3d4d3a24e9"></a>`additionalProperties`: `false`
- <a id="s-2526acc199"></a>`title`: `"EvaluationReviewIn"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-521cc519ce"></a>`note` | no | anyOf=[(type="string"; maxLength=4000; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$"); (type="null")]; title="Note" |  |
| <a id="s-ea089efab5"></a>`rating` | no | anyOf=[(type="integer"; minimum=1; maximum=5); (type="null")]; title="Rating" |  |

### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `anyOf` alternative 1](#s-16e9e7f805) |
| 2 | [See `anyOf` alternative 2](#s-975da0f24a) |

### <a id="s-16e9e7f805"></a>`anyOf` alternative 1

- <a id="s-91f704a6d3"></a>`required`: `["rating"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-75e29733b7"></a>`rating` | yes | type="integer" |  |

### <a id="s-975da0f24a"></a>`anyOf` alternative 2

- <a id="s-be65350add"></a>`required`: `["note"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b0a28c5daf"></a>`note` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b314ade763"></a>[field note · string value](#s-521cc519ce) | `length · characters · contract_max` | maximum=4000 |
| <a id="s-9f0160af01"></a>[field rating · integer value](#s-ea089efab5) | `value · schema-value · contract_max` | maximum=5 |

## Governing policies

- <a id="pa-9922a91cb8"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e9568f4d7c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationReviewIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22ba7fe36a5ebd9409bb48d166bbeac955c2bf05eeb07b27d5a6185157f277a3 -->

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
  "title": "EvaluationReviewIn",
  "type": "object"
}
```

</details>
