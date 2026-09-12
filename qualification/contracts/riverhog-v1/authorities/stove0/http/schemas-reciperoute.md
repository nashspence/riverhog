# schemas: RecipeRoute

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-reciperoute:fdf6aa9229 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeRoute`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactRule](schemas-artifactrule.md)
- [schemas: FactPredicate](schemas-factpredicate.md)
- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: OperationProjection](schemas-operationprojection.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: RecipeRoute
- `description`: One ordinary target/effect leaf selected by a recipe.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_rules` | no | array |  |
| `associated_roles` | no | array |  |
| `id` | yes | string |  |
| `input_retrieval_policy` | no | string |  |
| `intent` | no | object |  |
| `kind` | no | string |  |
| `operation_id` | yes | string |  |
| `primary_role` | no | object (2 fields) |  |
| `projections` | no | array |  |
| `target_options` | no | object |  |
| `target_registration_id` | yes | string |  |
| `when` | no | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 66f91c4ae17d3ad696e21e39a8d8b2f25d3753b08da095090b8c24966f6a8728 -->

```json
{
  "additionalProperties": false,
  "description": "One ordinary target/effect leaf selected by a recipe.",
  "properties": {
    "artifact_rules": {
      "default": [
        {
          "glob": "*",
          "role": "stove0.source/v1"
        }
      ],
      "items": {
        "$ref": "#/components/schemas/ArtifactRule"
      },
      "title": "Artifact Rules",
      "type": "array"
    },
    "associated_roles": {
      "default": [],
      "items": {
        "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
        "type": "string"
      },
      "title": "Associated Roles",
      "type": "array"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "input_retrieval_policy": {
      "default": "available-only",
      "enum": [
        "available-only",
        "allow"
      ],
      "title": "Input Retrieval Policy",
      "type": "string"
    },
    "intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Intent",
      "type": "object"
    },
    "kind": {
      "const": "operation",
      "default": "operation",
      "title": "Kind",
      "type": "string"
    },
    "operation_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Operation Id",
      "type": "string"
    },
    "primary_role": {
      "anyOf": [
        {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Primary Role"
    },
    "projections": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/OperationProjection"
      },
      "title": "Projections",
      "type": "array"
    },
    "target_options": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Target Options",
      "type": "object"
    },
    "target_registration_id": {
      "title": "Target Registration Id",
      "type": "string"
    },
    "when": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/FactPredicate"
      },
      "title": "When",
      "type": "array"
    }
  },
  "required": [
    "id",
    "operation_id",
    "target_registration_id"
  ],
  "title": "RecipeRoute",
  "type": "object"
}
```
