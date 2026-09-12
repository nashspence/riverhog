# schemas: EvaluationDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationdefinition:29c20edb8a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: EvaluationDefinition
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `common_intent` | no | type="object"; additional keys=`additionalProperties` |  |
| `evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `format` | no | type="string"; const="stove0-evaluation-definition/v1" |  |
| `inputs` | yes | type="array"; minItems=1; items=(#/components/schemas/CollectionRootRef) |  |
| `matrix` | yes | #/components/schemas/EvaluationMatrix |  |
| `purpose` | no | type="string"; enum=["trial","evaluation"] |  |
| `recipe` | yes | #/components/schemas/RecipeRef |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionRootRef](schemas-collectionrootref.md)
- [schemas: EvaluationMatrix](schemas-evaluationmatrix.md)
- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: RecipeRef](schemas-reciperef.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
