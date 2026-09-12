# schemas: WorkIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workidentity:4e2f338b3e -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkIdentity`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: BranchWorkBinding](schemas-branchworkbinding.md)
- [schemas: CollectionRootRef](schemas-collectionrootref.md)
- [schemas: EvaluationBinding](schemas-evaluationbinding.md)
- [schemas: JoinWorkBinding](schemas-joinworkbinding.md)
- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: RecipeRef](schemas-reciperef.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: WorkIdentity
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `effective_intent` | no | object |  |
| `evaluation` | no | object (1 fields) |  |
| `fork_join` | no | object (2 fields) |  |
| `format` | no | string |  |
| `inputs` | yes | array |  |
| `recipe` | yes | #/components/schemas/RecipeRef |  |
| `work_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a16e4895513ac18dd61fd4a29fd1b576cb62b2fc14b92e330784efec26186c4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "effective_intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Effective Intent",
      "type": "object"
    },
    "evaluation": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/EvaluationBinding"
        },
        {
          "type": "null"
        }
      ]
    },
    "fork_join": {
      "anyOf": [
        {
          "discriminator": {
            "mapping": {
              "branch": "#/components/schemas/BranchWorkBinding",
              "join": "#/components/schemas/JoinWorkBinding"
            },
            "propertyName": "kind"
          },
          "oneOf": [
            {
              "$ref": "#/components/schemas/BranchWorkBinding"
            },
            {
              "$ref": "#/components/schemas/JoinWorkBinding"
            }
          ]
        },
        {
          "type": "null"
        }
      ],
      "title": "Fork Join"
    },
    "format": {
      "const": "stove0-work/v1",
      "default": "stove0-work/v1",
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
    "recipe": {
      "$ref": "#/components/schemas/RecipeRef"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "recipe",
    "inputs",
    "work_id"
  ],
  "title": "WorkIdentity",
  "type": "object"
}
```
