# schemas: RecipeView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-recipeview:5760401dec -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-a03e326ae1"></a>

- <a id="s-4451e21cdd"></a>`type`: `"object"`
- <a id="s-9bf4575ad5"></a>`additionalProperties`: `false`
- <a id="s-8487eabd29"></a>`required`: `["definition","sha256"]`
- <a id="s-22352b44b1"></a>`title`: `"RecipeView"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1a57e1956e"></a>`definition` | yes | [RecipeDefinition](schemas-recipedefinition.md) |  |
| <a id="s-c5b9d49560"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-c5b9d49560) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [RecipeDefinition](schemas-recipedefinition.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-01c42e64b9"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-ee8408c2c6"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0fb10c4f0bc4fa85299c43525cd92cfb7f971e991eb10d48453846545d46029a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "definition": {
      "$ref": "#/components/schemas/RecipeDefinition"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "definition",
    "sha256"
  ],
  "title": "RecipeView",
  "type": "object"
}
```

</details>
