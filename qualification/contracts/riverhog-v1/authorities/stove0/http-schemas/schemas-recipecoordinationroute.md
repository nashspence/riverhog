# schemas: RecipeCoordinationRoute

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-recipecoordinationroute:91823c033d -->

One exact subrecipe selected as a branch-bound coordinator.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-49c4834612"></a>

- <a id="s-fec7e8204d"></a>`type`: `"object"`
- <a id="s-688e24598d"></a>`additionalProperties`: `false`
- <a id="s-b5f335c1e3"></a>`description`: `"One exact subrecipe selected as a branch-bound coordinator."`
- <a id="s-92a3458aab"></a>`required`: `["id","recipe"]`
- <a id="s-5024797bd0"></a>`title`: `"RecipeCoordinationRoute"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d85117f452"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","role":"stove0.source/v1"}]; items=([ArtifactRule](schemas-artifactrule.md)); title="Artifact Rules" |  |
| <a id="s-700d57d1c0"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Associated Roles" |  |
| <a id="s-1e59584371"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-755ac34ea8"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Intent" |  |
| <a id="s-028eb1ca79"></a>`kind` | no | type="string"; const="coordination"; default="coordination"; title="Kind" |  |
| <a id="s-0b22e4cc4a"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; title="Primary Role" |  |
| <a id="s-2deb6d5c81"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](schemas-operationprojection.md)); title="Projections" |  |
| <a id="s-c91319df09"></a>`recipe` | yes | [RecipeIdentityRef](schemas-recipeidentityref.md) |  |
| <a id="s-4c285d7366"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](schemas-factpredicate.md)); title="When" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_rules](#s-d85117f452) | `cardinality · items · operational_policy` | shared above |
| [field associated_roles](#s-700d57d1c0) | `cardinality · items · operational_policy` | shared above |
| [field intent](#s-755ac34ea8) | `cardinality · entries · operational_policy` | shared above |
| [field projections](#s-2deb6d5c81) | `cardinality · items · operational_policy` | shared above |
| [field when](#s-4c285d7366) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArtifactRule](schemas-artifactrule.md)
- [FactPredicate](schemas-factpredicate.md)
- [JsonValue](schemas-jsonvalue.md)
- [OperationProjection](schemas-operationprojection.md)
- [RecipeIdentityRef](schemas-recipeidentityref.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-10a5e6b5ed"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-f308e3a18b"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeCoordinationRoute`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 46d9df2afe51dd102cc6e403ac7ad5fb53ba251f69b0f85d4c6a4e0b8311e5e9 -->

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
      "$ref": "#/components/schemas/RecipeIdentityRef"
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

</details>
