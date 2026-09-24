# schemas: OperatorWorkflowPreviewRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-operatorworkflowpreviewrequest:9d65ff3700 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-9381f89cbc"></a>

- <a id="s-8f6861668b"></a>`type`: `"object"`
- <a id="s-9ac0fe3990"></a>`additionalProperties`: `false`
- <a id="s-4e9345850f"></a>`required`: `["recipe_id","inputs"]`
- <a id="s-c9577ab9d8"></a>`title`: `"OperatorWorkflowPreviewRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d8a5ccc404"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Effective Intent" |  |
| <a id="s-7842431ae4"></a>`inputs` | yes | type="array"; items=([CollectionRootIdentityRef](schemas-collectionrootidentityref.md)); minItems=1; title="Inputs" |  |
| <a id="s-187cc83d3f"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1; title="Recipe Id" |  |
| <a id="s-882876825e"></a>`recipe_revision` | no | anyOf=[([NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1); (type="null")] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effective_intent](#s-d8a5ccc404) | `cardinality · entries · operational_policy` | shared above |
| [field inputs](#s-7842431ae4) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=160; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field recipe_id](#s-187cc83d3f) | `length · characters · contract_max` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionRootIdentityRef](schemas-collectionrootidentityref.md)
- [JsonValue](schemas-jsonvalue.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-406b9ac451"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-10c0ca5efc"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-8c39d42b90"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/OperatorWorkflowPreviewRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c6d128e6ed888575a2c489e91a92a1e6c215cfaa2cd81056708c768d8f9745d -->

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
    "inputs"
  ],
  "title": "OperatorWorkflowPreviewRequest",
  "type": "object"
}
```

</details>
