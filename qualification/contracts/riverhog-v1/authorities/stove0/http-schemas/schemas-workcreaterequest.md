# schemas: WorkCreateRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workcreaterequest:092cab26b9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-0a9a1fbf4e"></a>

- <a id="s-4e56333784"></a>`type`: `"object"`
- <a id="s-9123605553"></a>`additionalProperties`: `false`
- <a id="s-3e7f836918"></a>`required`: `["recipe_id","inputs","preview_sha256"]`
- <a id="s-2c64a8d97a"></a>`title`: `"WorkCreateRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4f20aa35aa"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Effective Intent" |  |
| <a id="s-7460335fa4"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](schemas-collectionrootidentityref.md)); minItems=1; title="Inputs" |  |
| <a id="s-005e3b22e5"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Preview Sha256" |  |
| <a id="s-6961dd98e0"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1; title="Recipe Id" |  |
| <a id="s-1315df63b9"></a>`recipe_revision` | no | anyOf=[([NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1); (type="null")] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effective_intent](#s-4f20aa35aa) | `cardinality · entries · operational_policy` | shared above |
| [field inputs](#s-7460335fa4) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field preview_sha256](#s-005e3b22e5) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field recipe_id](#s-6961dd98e0) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract elements

- [CollectionRootIdentityRef](schemas-collectionrootidentityref.md)
- [JsonValue](schemas-jsonvalue.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-fd114d7924"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e78c0d00fc"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-1df5d26ccf"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkCreateRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 049e053df1178dff8db548d3d04370905ff424da378424f344b5ffce4787bed8 -->

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
        "$ref": "#/components/schemas/CollectionRootIdentityRef"
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
          "$ref": "#/components/schemas/NonnegativeDecimal",
          "ge": 1
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "recipe_id",
    "inputs",
    "preview_sha256"
  ],
  "title": "WorkCreateRequest",
  "type": "object"
}
```

</details>
