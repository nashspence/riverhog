# schemas: EvaluationView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationview:2ec5062688 -->

Operator projection of a materialized evaluation, not its identity.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-230a00e66668"></a>
- <a id="s-4a4c8bc73929"></a>`title`: EvaluationView
- <a id="s-9abb8c314858"></a>`description`: Operator projection of a materialized evaluation, not its identity.
- <a id="s-f311aef2adb8"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f420db184156"></a>`children` | yes | type="array"; items=(#/components/schemas/EvaluationChildView) |  |
| <a id="s-89f936baba35"></a>`definition` | yes | #/components/schemas/EvaluationDefinition |  |
| <a id="s-004a345cd7c4"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-55a799b1af38"></a>`format` | no | type="string"; const="stove0-evaluation-view/v1" |  |
| <a id="s-bc4fcb4992ea"></a>`phase` | yes | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"] |  |
| <a id="s-ca1a0eb70f65"></a>`reviews` | no | type="array"; items=(#/components/schemas/EvaluationReviewView) |  |
| <a id="s-f04cdee067ff"></a>`revision` | yes | type="integer"; minimum=1 |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field children](#s-f420db184156) | `cardinality · items · operational_policy` | shared above |
| [field reviews](#s-ca1a0eb70f65) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field evaluation_id](#s-004a345cd7c4) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: EvaluationChildView](schemas-evaluationchildview.md)
- [schemas: EvaluationDefinition](schemas-evaluationdefinition.md)
- [schemas: EvaluationReviewView](schemas-evaluationreviewview.md)

## Governing policies

- <a id="pa-ca295a437052"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-e8dfd55db47c"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-9906eedc09ca"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 94f5278bcc3670da961904a0eec89e1172e5874922b8ab76ff47c80dae0703e3 -->

```json
{
  "additionalProperties": false,
  "description": "Operator projection of a materialized evaluation, not its identity.",
  "properties": {
    "children": {
      "items": {
        "$ref": "#/components/schemas/EvaluationChildView"
      },
      "title": "Children",
      "type": "array"
    },
    "definition": {
      "$ref": "#/components/schemas/EvaluationDefinition"
    },
    "evaluation_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Evaluation Id",
      "type": "string"
    },
    "format": {
      "const": "stove0-evaluation-view/v1",
      "default": "stove0-evaluation-view/v1",
      "title": "Format",
      "type": "string"
    },
    "phase": {
      "enum": [
        "planning",
        "running",
        "partially_complete",
        "complete",
        "failed",
        "canceled"
      ],
      "title": "Phase",
      "type": "string"
    },
    "reviews": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/EvaluationReviewView"
      },
      "title": "Reviews",
      "type": "array"
    },
    "revision": {
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    }
  },
  "required": [
    "evaluation_id",
    "definition",
    "phase",
    "revision",
    "children"
  ],
  "title": "EvaluationView",
  "type": "object"
}
```
