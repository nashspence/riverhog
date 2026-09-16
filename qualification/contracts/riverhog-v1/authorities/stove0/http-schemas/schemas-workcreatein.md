# schemas: WorkCreateIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workcreatein:5dcd3147f0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4c057c9530"></a>

- <a id="s-4d3821068e"></a>`type`: `"object"`
- <a id="s-c22fac1be1"></a>`additionalProperties`: `false`
- <a id="s-0b411787dd"></a>`required`: `["recipe_id","inputs","preview_sha256"]`
- <a id="s-558de7a97d"></a>`title`: `"WorkCreateIn"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ad0ebd2307"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Effective Intent" |  |
| <a id="s-73fa49c400"></a>`inputs` | yes | type="array"; items=([CollectionRootRef](schemas-collectionrootref.md)); minItems=1; title="Inputs" |  |
| <a id="s-6883763dc1"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Preview Sha256" |  |
| <a id="s-1e56ede547"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1; title="Recipe Id" |  |
| <a id="s-cc2aeb54a6"></a>`recipe_revision` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; title="Recipe Revision" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effective_intent](#s-ad0ebd2307) | `cardinality · entries · operational_policy` | shared above |
| [field inputs](#s-73fa49c400) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field preview_sha256](#s-6883763dc1) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field recipe_id](#s-1e56ede547) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [CollectionRootRef](schemas-collectionrootref.md)
- [JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-63d853fd3b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-4344e67c1e"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-b0ee03247f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkCreateIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 817563604e4e4841d32e612eadcd1d367bfb11d3b8004d487f777820ff9b893e -->

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
    "inputs": {
      "items": {
        "$ref": "#/components/schemas/CollectionRootRef"
      },
      "minItems": 1,
      "title": "Inputs",
      "type": "array"
    },
    "preview_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Preview Sha256",
      "type": "string"
    },
    "recipe_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Recipe Id",
      "type": "string"
    },
    "recipe_revision": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Recipe Revision"
    }
  },
  "required": [
    "recipe_id",
    "inputs",
    "preview_sha256"
  ],
  "title": "WorkCreateIn",
  "type": "object"
}
```

</details>
