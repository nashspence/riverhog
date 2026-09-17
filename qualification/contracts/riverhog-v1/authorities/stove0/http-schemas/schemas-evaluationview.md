# schemas: EvaluationView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-evaluationview:89421ebede -->

Operator projection of a materialized evaluation, not its identity.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-230a00e666"></a>

- <a id="s-f311aef2ad"></a>`type`: `"object"`
- <a id="s-5d6a16f44f"></a>`additionalProperties`: `false`
- <a id="s-9abb8c3148"></a>`description`: `"Operator projection of a materialized evaluation, not its identity."`
- <a id="s-80ff4a235c"></a>`required`: `["evaluation_id","definition","phase","revision","children"]`
- <a id="s-4a4c8bc739"></a>`title`: `"EvaluationView"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f420db1841"></a>`children` | yes | type="array"; items=([EvaluationChildView](schemas-evaluationchildview.md)); title="Children" |  |
| <a id="s-89f936baba"></a>`definition` | yes | [EvaluationDefinition](schemas-evaluationdefinition.md) |  |
| <a id="s-004a345cd7"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Evaluation Id" |  |
| <a id="s-55a799b1af"></a>`format` | no | type="string"; const="stove0-evaluation-view/v1"; default="stove0-evaluation-view/v1"; title="Format" |  |
| <a id="s-bc4fcb4992"></a>`phase` | yes | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"]; title="Phase" |  |
| <a id="s-ca1a0eb70f"></a>`reviews` | no | type="array"; default=[]; items=([EvaluationReviewView](schemas-evaluationreviewview.md)); title="Reviews" |  |
| <a id="s-f04cdee067"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field children](#s-f420db1841) | `cardinality · items · operational_policy` | shared above |
| [field reviews](#s-ca1a0eb70f) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field evaluation_id](#s-004a345cd7) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [EvaluationChildView](schemas-evaluationchildview.md)
- [EvaluationDefinition](schemas-evaluationdefinition.md)
- [EvaluationReviewView](schemas-evaluationreviewview.md)

## Governing policies

- <a id="pa-c2b4ec112b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-edcfcc28ca"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-6eae3747de"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
