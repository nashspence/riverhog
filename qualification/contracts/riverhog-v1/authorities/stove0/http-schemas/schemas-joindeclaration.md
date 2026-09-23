# schemas: JoinDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-joindeclaration:b6f1f563af -->

One optional exact named-subset join declaration.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-5693acffb6"></a>

- <a id="s-5f45fed464"></a>`type`: `"object"`
- <a id="s-ad3ca898ce"></a>`additionalProperties`: `false`
- <a id="s-422859882c"></a>`description`: `"One optional exact named-subset join declaration."`
- <a id="s-154418823d"></a>`required`: `["members","recipe","workflow_intent","join_declaration_sha256"]`
- <a id="s-bca1349211"></a>`title`: `"JoinDeclaration"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eca86f5eae"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Effective Intent" |  |
| <a id="s-31d9aefee4"></a>`format` | no | type="string"; const="stove0-join-declaration/v1"; default="stove0-join-declaration/v1"; title="Format" |  |
| <a id="s-a8773a6c8d"></a>`join_declaration_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Join Declaration Sha256" |  |
| <a id="s-23dfcec269"></a>`members` | yes | type="array"; items=([JoinMemberDeclaration](schemas-joinmemberdeclaration.md)); minItems=2; title="Members" |  |
| <a id="s-df2b4dd551"></a>`recipe` | yes | [RecipeRef](schemas-reciperef.md) |  |
| <a id="s-6e6a700a1a"></a>`workflow_intent` | yes | [WorkflowPlanIntent](schemas-workflowplanintent.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effective_intent](#s-eca86f5eae) | `cardinality · entries · operational_policy` | shared above |
| [field members](#s-23dfcec269) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field join_declaration_sha256](#s-a8773a6c8d) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [JoinMemberDeclaration](schemas-joinmemberdeclaration.md)
- [JsonValue](schemas-jsonvalue.md)
- [RecipeRef](schemas-reciperef.md)
- [WorkflowPlanIntent](schemas-workflowplanintent.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-af666d7056"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-59becc8fea"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-6fec004de5"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinDeclaration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
