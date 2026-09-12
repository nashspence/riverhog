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

<a id="s-ff8515a74c"></a>
- <a id="s-b033d3afe8"></a>`title`: RecipeRoute
- <a id="s-b1fa805af6"></a>`description`: One ordinary target/effect leaf selected by a recipe.
- <a id="s-46086beeb2"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a9eea0d359"></a>`artifact_rules` | no | type="array"; items=(#/components/schemas/ArtifactRule) |  |
| <a id="s-1219143978"></a>`associated_roles` | no | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-b95e5c198f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ba31dee9dd"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"] |  |
| <a id="s-933eae7c5b"></a>`intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-e07a66df92"></a>`kind` | no | type="string"; const="operation" |  |
| <a id="s-92aa53f071"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-02f69c80ce"></a>`primary_role` | no | anyOf=type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" \| type="null" |  |
| <a id="s-0216849edb"></a>`projections` | no | type="array"; items=(#/components/schemas/OperationProjection) |  |
| <a id="s-fd18a88ac7"></a>`target_options` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-49924f12d8"></a>`target_registration_id` | yes | type="string" |  |
| <a id="s-10fc7e90e5"></a>`when` | no | type="array"; items=(#/components/schemas/FactPredicate) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_rules](#s-a9eea0d359) | `cardinality · items · operational_policy` | shared above |
| [field associated_roles](#s-1219143978) | `cardinality · items · operational_policy` | shared above |
| [field intent](#s-933eae7c5b) | `cardinality · entries · operational_policy` | shared above |
| [field projections](#s-0216849edb) | `cardinality · items · operational_policy` | shared above |
| [field target_options](#s-fd18a88ac7) | `cardinality · entries · operational_policy` | shared above |
| [field when](#s-10fc7e90e5) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactRule](schemas-artifactrule.md)
- [schemas: FactPredicate](schemas-factpredicate.md)
- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: OperationProjection](schemas-operationprojection.md)

## Governing policies

- <a id="pa-bcb8d4f9ca"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-28fcce1d62"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
