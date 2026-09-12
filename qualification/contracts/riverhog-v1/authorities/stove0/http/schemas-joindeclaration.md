# schemas: JoinDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-joindeclaration:b575d2f468 -->

One optional exact named-subset join declaration.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: JoinDeclaration
- `description`: One optional exact named-subset join declaration.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `effective_intent` | no | type="object"; additional keys=`additionalProperties` |  |
| `format` | no | type="string"; const="stove0-join-declaration/v1" |  |
| `join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `members` | yes | type="array"; minItems=2; items=(#/components/schemas/JoinMemberDeclaration) |  |
| `recipe` | yes | #/components/schemas/RecipeRef |  |
| `workflow_intent` | yes | #/components/schemas/WorkflowPlanIntent |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JoinMemberDeclaration](schemas-joinmemberdeclaration.md)
- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: RecipeRef](schemas-reciperef.md)
- [schemas: WorkflowPlanIntent](schemas-workflowplanintent.md)

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

- `/external_contract/http_openapi/stove0/components/schemas/JoinDeclaration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f5546eba1b0ee2da6a3a1215a3422b2bbd3ea9083d64eff4344c803b089b9ecb -->

```json
{
  "additionalProperties": false,
  "description": "One optional exact named-subset join declaration.",
  "properties": {
    "effective_intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Effective Intent",
      "type": "object"
    },
    "format": {
      "const": "stove0-join-declaration/v1",
      "default": "stove0-join-declaration/v1",
      "title": "Format",
      "type": "string"
    },
    "join_declaration_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Join Declaration Sha256",
      "type": "string"
    },
    "members": {
      "items": {
        "$ref": "#/components/schemas/JoinMemberDeclaration"
      },
      "minItems": 2,
      "title": "Members",
      "type": "array"
    },
    "recipe": {
      "$ref": "#/components/schemas/RecipeRef"
    },
    "workflow_intent": {
      "$ref": "#/components/schemas/WorkflowPlanIntent"
    }
  },
  "required": [
    "members",
    "recipe",
    "workflow_intent",
    "join_declaration_sha256"
  ],
  "title": "JoinDeclaration",
  "type": "object"
}
```
