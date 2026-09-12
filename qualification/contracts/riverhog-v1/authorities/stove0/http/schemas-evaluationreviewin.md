# schemas: EvaluationReviewIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationreviewin:e6d7e04264 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-d1a5ea0009ee"></a>
- <a id="s-2526acc1994a"></a>`title`: EvaluationReviewIn
- <a id="s-68c174083dce"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-521cc519ce03"></a>`note` | no | anyOf=type="string"; minLength=1; maxLength=4000; pattern="^\\S(?:[\\s\\S]*\\S)?$" \| type="null" |  |
| <a id="s-ea089efab5ad"></a>`rating` | no | anyOf=type="integer"; minimum=1; maximum=5 \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b314ade7635f"></a>field note · anyOf alternative 1 | `length · characters · contract_max` | maximum=4000 |
| <a id="s-9f0160af012d"></a>field rating · anyOf alternative 1 | `value · schema-value · contract_max` | maximum=5 |

## Governing policies

- <a id="pa-310d2ea59ace"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-9bb264747b28"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationReviewIn`

### Exact owned JSON

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
