# schemas: JoinDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-joindeclaration:b575d2f468 -->

One optional exact named-subset join declaration.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-5693acffb6"></a>
- <a id="s-bca1349211"></a>`title`: JoinDeclaration
- <a id="s-422859882c"></a>`description`: One optional exact named-subset join declaration.
- <a id="s-5f45fed464"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eca86f5eae"></a>`effective_intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-31d9aefee4"></a>`format` | no | type="string"; const="stove0-join-declaration/v1" |  |
| <a id="s-a8773a6c8d"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-23dfcec269"></a>`members` | yes | type="array"; minItems=2; items=(#/components/schemas/JoinMemberDeclaration) |  |
| <a id="s-df2b4dd551"></a>`recipe` | yes | #/components/schemas/RecipeRef |  |
| <a id="s-6e6a700a1a"></a>`workflow_intent` | yes | #/components/schemas/WorkflowPlanIntent |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effective_intent](#s-eca86f5eae) | `cardinality · entries · operational_policy` | shared above |
| [field members](#s-23dfcec269) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field join_declaration_sha256](#s-a8773a6c8d) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JoinMemberDeclaration](schemas-joinmemberdeclaration.md)
- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: RecipeRef](schemas-reciperef.md)
- [schemas: WorkflowPlanIntent](schemas-workflowplanintent.md)

## Governing policies

- <a id="pa-be8cac4cc5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-b67372c34c"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-fdd06d904e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
