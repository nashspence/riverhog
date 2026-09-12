# schemas: RecipeCoordinationRoute

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-recipecoordinationroute:fc76361359 -->

One exact subrecipe selected as a branch-bound coordinator.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-49c4834612"></a>
- <a id="s-5024797bd0"></a>`title`: RecipeCoordinationRoute
- <a id="s-b5f335c1e3"></a>`description`: One exact subrecipe selected as a branch-bound coordinator.
- <a id="s-fec7e8204d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d85117f452"></a>`artifact_rules` | no | type="array"; items=(#/components/schemas/ArtifactRule) |  |
| <a id="s-700d57d1c0"></a>`associated_roles` | no | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-1e59584371"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-755ac34ea8"></a>`intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-028eb1ca79"></a>`kind` | no | type="string"; const="coordination" |  |
| <a id="s-0b22e4cc4a"></a>`primary_role` | no | anyOf=type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" \| type="null" |  |
| <a id="s-2deb6d5c81"></a>`projections` | no | type="array"; items=(#/components/schemas/OperationProjection) |  |
| <a id="s-c91319df09"></a>`recipe` | yes | #/components/schemas/RecipeRef |  |
| <a id="s-4c285d7366"></a>`when` | no | type="array"; items=(#/components/schemas/FactPredicate) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_rules](#s-d85117f452) | `cardinality · items · operational_policy` | shared above |
| [field associated_roles](#s-700d57d1c0) | `cardinality · items · operational_policy` | shared above |
| [field intent](#s-755ac34ea8) | `cardinality · entries · operational_policy` | shared above |
| [field projections](#s-2deb6d5c81) | `cardinality · items · operational_policy` | shared above |
| [field when](#s-4c285d7366) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactRule](schemas-artifactrule.md)
- [schemas: FactPredicate](schemas-factpredicate.md)
- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: OperationProjection](schemas-operationprojection.md)
- [schemas: RecipeRef](schemas-reciperef.md)

## Governing policies

- <a id="pa-51a0ae3ce4"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-428dd31dbc"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeCoordinationRoute`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 624a1e3bbf0ecc59de56616c6389216c4ec53fe24f850f44b3a8af409d11280d -->

```json
{
  "additionalProperties": false,
  "description": "One exact subrecipe selected as a branch-bound coordinator.",
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
    "intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Intent",
      "type": "object"
    },
    "kind": {
      "const": "coordination",
      "default": "coordination",
      "title": "Kind",
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
    "recipe": {
      "$ref": "#/components/schemas/RecipeRef"
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
    "recipe"
  ],
  "title": "RecipeCoordinationRoute",
  "type": "object"
}
```
