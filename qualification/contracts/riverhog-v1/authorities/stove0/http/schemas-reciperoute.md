# schemas: RecipeRoute

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-reciperoute:fdf6aa9229 -->

One ordinary target/effect leaf selected by a recipe.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 6 |

## External contract

<a id="s-ff8515a74ce8"></a>
- <a id="s-b033d3afe8cd"></a>`title`: RecipeRoute
- <a id="s-b1fa805af69a"></a>`description`: One ordinary target/effect leaf selected by a recipe.
- <a id="s-46086beeb2fc"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a9eea0d35955"></a>`artifact_rules` | no | type="array"; items=(#/components/schemas/ArtifactRule) |  |
| <a id="s-12191439789c"></a>`associated_roles` | no | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-b95e5c198fd2"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ba31dee9dd95"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"] |  |
| <a id="s-933eae7c5b6c"></a>`intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-e07a66df92bc"></a>`kind` | no | type="string"; const="operation" |  |
| <a id="s-92aa53f0715e"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-02f69c80ce89"></a>`primary_role` | no | anyOf=type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" \| type="null" |  |
| <a id="s-0216849edb8b"></a>`projections` | no | type="array"; items=(#/components/schemas/OperationProjection) |  |
| <a id="s-fd18a88ac70a"></a>`target_options` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-49924f12d800"></a>`target_registration_id` | yes | type="string" |  |
| <a id="s-10fc7e90e52b"></a>`when` | no | type="array"; items=(#/components/schemas/FactPredicate) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_rules](#s-a9eea0d35955) | `cardinality · items · operational_policy` | shared above |
| [field associated_roles](#s-12191439789c) | `cardinality · items · operational_policy` | shared above |
| [field intent](#s-933eae7c5b6c) | `cardinality · entries · operational_policy` | shared above |
| [field projections](#s-0216849edb8b) | `cardinality · items · operational_policy` | shared above |
| [field target_options](#s-fd18a88ac70a) | `cardinality · entries · operational_policy` | shared above |
| [field when](#s-10fc7e90e52b) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactRule](schemas-artifactrule.md)
- [schemas: FactPredicate](schemas-factpredicate.md)
- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: OperationProjection](schemas-operationprojection.md)

## Governing policies

- <a id="pa-bcb8d4f9ca55"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-28fcce1d6251"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeRoute`

### Exact owned JSON

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
