# schemas: WorkIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workidentity:acc36fd82e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4f74f11a9b"></a>

- <a id="s-7da3819b2c"></a>`type`: `"object"`
- <a id="s-f81d32d691"></a>`additionalProperties`: `false`
- <a id="s-e7a02a4d86"></a>`required`: `["recipe","inputs","work_id"]`
- <a id="s-772bd198cf"></a>`title`: `"WorkIdentity"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-94dc797a9d"></a>`effective_intent` | no | type="object"; additionalProperties=(#/components/schemas/JsonValue) |  |
| <a id="s-1f8b8e35e0"></a>`evaluation` | no | anyOf=(#/components/schemas/EvaluationBinding) \| (type="null") |  |
| <a id="s-627ac6d042"></a>`fork_join` | no | anyOf=(discriminator={"mapping":{"branch":"#/components/schemas/BranchWorkBinding","join":"#/components/schemas/JoinWorkBinding"},"propertyName":"kind"}; oneOf=(#/components/schemas/BranchWorkBinding) \| (#/components/schemas/JoinWorkBinding)) \| (type="null") |  |
| <a id="s-3b7d02e296"></a>`format` | no | type="string"; const="stove0-work/v1"; default="stove0-work/v1" |  |
| <a id="s-181af1fa7a"></a>`inputs` | yes | type="array"; items=(#/components/schemas/CollectionRootRef); minItems=1 |  |
| <a id="s-adf4a103e4"></a>`recipe` | yes | #/components/schemas/RecipeRef |  |
| <a id="s-81ce230f85"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effective_intent](#s-94dc797a9d) | `cardinality · entries · operational_policy` | shared above |
| [field inputs](#s-181af1fa7a) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field work_id](#s-81ce230f85) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [BranchWorkBinding](schemas-branchworkbinding.md)
- [CollectionRootRef](schemas-collectionrootref.md)
- [EvaluationBinding](schemas-evaluationbinding.md)
- [JoinWorkBinding](schemas-joinworkbinding.md)
- [JsonValue](schemas-jsonvalue.md)
- [RecipeRef](schemas-reciperef.md)

## Governing policies

- <a id="pa-8c906a342d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-cb25a4a821"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-253bdb5ca9"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
