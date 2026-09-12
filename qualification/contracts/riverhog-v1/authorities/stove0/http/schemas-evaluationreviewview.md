# schemas: EvaluationReviewView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationreviewview:969db1f909 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-96bb03a6cd"></a>
- <a id="s-263c0d8f50"></a>`title`: EvaluationReviewView
- <a id="s-73aadeda50"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-24c43eadff"></a>`note` | no | anyOf=type="string"; minLength=1; maxLength=4000; pattern="^\\S(?:[\\s\\S]*\\S)?$" \| type="null" |  |
| <a id="s-f75f344358"></a>`rating` | no | anyOf=type="integer"; minimum=1; maximum=5 \| type="null" |  |
| <a id="s-689703e9c5"></a>`updated_at` | yes | type="string"; minLength=1; maxLength=40 |  |
| <a id="s-2d55a4ad50"></a>`updated_by` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-27564cd680"></a>`variant_id` | yes | type="string"; minLength=1; maxLength=160 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-3480c044fe"></a>[field note · string value](#s-24c43eadff) | `length · characters · contract_max` | maximum=4000 |
| <a id="s-b427e4e826"></a>[field rating · integer value](#s-f75f344358) | `value · schema-value · contract_max` | maximum=5 |
| [field updated_at](#s-689703e9c5) | `length · characters · contract_max` | maximum=40 |
| [field updated_by](#s-2d55a4ad50) | `length · characters · contract_max` | maximum=160 |
| [field variant_id](#s-27564cd680) | `length · characters · contract_max` | maximum=160 |

## Governing policies

- <a id="pa-e5fbcd1f26"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-ab4ff43cb5"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationReviewView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b523bdd6e34cda0cc29b30ba551749cf3dc07b5c971d74a87b8906f5961e1117 -->

```json
{
  "additionalProperties": false,
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
    },
    "updated_at": {
      "maxLength": 40,
      "minLength": 1,
      "title": "Updated At",
      "type": "string"
    },
    "updated_by": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Updated By",
      "type": "string"
    },
    "variant_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Variant Id",
      "type": "string"
    }
  },
  "required": [
    "variant_id",
    "updated_by",
    "updated_at"
  ],
  "title": "EvaluationReviewView",
  "type": "object"
}
```
