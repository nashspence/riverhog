# schemas: EvaluationDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-evaluationdefinition:c11e61c385 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-60000b3b20"></a>

- <a id="s-ebc2c2cae7"></a>`type`: `"object"`
- <a id="s-064872d3db"></a>`additionalProperties`: `false`
- <a id="s-8576cd8e08"></a>`required`: `["recipe","inputs","matrix","evaluation_id"]`
- <a id="s-822697a1bd"></a>`title`: `"EvaluationDefinition"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed083ea0aa"></a>`common_intent` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Common Intent" |  |
| <a id="s-1d84fffa86"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Evaluation Id" |  |
| <a id="s-d003b4a71c"></a>`format` | no | type="string"; const="stove0-evaluation-definition/v1"; default="stove0-evaluation-definition/v1"; title="Format" |  |
| <a id="s-52ffc0be04"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](schemas-collectionrootidentityref.md)); minItems=1; title="Inputs" |  |
| <a id="s-bf750d4125"></a>`matrix` | yes | [EvaluationMatrix](schemas-evaluationmatrix.md) |  |
| <a id="s-ec1c535372"></a>`purpose` | no | type="string"; enum=["trial","evaluation"]; default="evaluation"; title="Purpose" |  |
| <a id="s-a02c55f45d"></a>`recipe` | yes | [RecipeIdentityRef](schemas-recipeidentityref.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field common_intent](#s-ed083ea0aa) | `cardinality · entries · operational_policy` | shared above |
| [field inputs](#s-52ffc0be04) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field evaluation_id](#s-1d84fffa86) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionRootIdentityRef](schemas-collectionrootidentityref.md)
- [EvaluationMatrix](schemas-evaluationmatrix.md)
- [JsonValue](schemas-jsonvalue.md)
- [RecipeIdentityRef](schemas-recipeidentityref.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-dff4fb3c84"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-04d628eea5"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-0cc9786c7a"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationDefinition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 45628de56393d1b2f0f9379bb73bd3bf50e0c9e6fcb214c1ef8fd3a5794d9049 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "common_intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Common Intent",
      "type": "object"
    },
    "evaluation_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Evaluation Id",
      "type": "string"
    },
    "format": {
      "const": "stove0-evaluation-definition/v1",
      "default": "stove0-evaluation-definition/v1",
      "title": "Format",
      "type": "string"
    },
    "inputs": {
      "items": {
        "$ref": "#/components/schemas/CollectionRootIdentityRef"
      },
      "minItems": 1,
      "title": "Inputs",
      "type": "array"
    },
    "matrix": {
      "$ref": "#/components/schemas/EvaluationMatrix"
    },
    "purpose": {
      "default": "evaluation",
      "enum": [
        "trial",
        "evaluation"
      ],
      "title": "Purpose",
      "type": "string"
    },
    "recipe": {
      "$ref": "#/components/schemas/RecipeIdentityRef"
    }
  },
  "required": [
    "recipe",
    "inputs",
    "matrix",
    "evaluation_id"
  ],
  "title": "EvaluationDefinition",
  "type": "object"
}
```

</details>
