# schemas: EvaluationDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationdefinition:29c20edb8a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-60000b3b20"></a>
- <a id="s-822697a1bd"></a>`title`: EvaluationDefinition
- <a id="s-ebc2c2cae7"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed083ea0aa"></a>`common_intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-1d84fffa86"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d003b4a71c"></a>`format` | no | type="string"; const="stove0-evaluation-definition/v1" |  |
| <a id="s-52ffc0be04"></a>`inputs` | yes | type="array"; minItems=1; items=(#/components/schemas/CollectionRootRef) |  |
| <a id="s-bf750d4125"></a>`matrix` | yes | #/components/schemas/EvaluationMatrix |  |
| <a id="s-ec1c535372"></a>`purpose` | no | type="string"; enum=["trial","evaluation"] |  |
| <a id="s-a02c55f45d"></a>`recipe` | yes | #/components/schemas/RecipeRef |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field common_intent](#s-ed083ea0aa) | `cardinality · entries · operational_policy` | shared above |
| [field inputs](#s-52ffc0be04) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field evaluation_id](#s-1d84fffa86) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionRootRef](schemas-collectionrootref.md)
- [schemas: EvaluationMatrix](schemas-evaluationmatrix.md)
- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: RecipeRef](schemas-reciperef.md)

## Governing policies

- <a id="pa-587bfa0e6e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-4929cf954f"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-7c120cc28a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationDefinition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 85776fbbcc3b08f22d7521fb69c1035be3f997f9c685220d179af7cd994e096a -->

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
        "$ref": "#/components/schemas/CollectionRootRef"
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
      "$ref": "#/components/schemas/RecipeRef"
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
