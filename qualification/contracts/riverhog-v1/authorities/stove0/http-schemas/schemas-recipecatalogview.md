# schemas: RecipeCatalogView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-recipecatalogview:c530328ebb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-5e95a96bce"></a>

- <a id="s-db8bd342c4"></a>`type`: `"object"`
- <a id="s-0206f8342c"></a>`additionalProperties`: `false`
- <a id="s-972b2d9356"></a>`required`: `["catalog_sha256","recipes"]`
- <a id="s-334b253208"></a>`title`: `"RecipeCatalogView"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-701d54ed95"></a>`catalog_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Catalog Sha256" |  |
| <a id="s-d36c79e332"></a>`recipes` | yes | type="array"; items=([RecipeView](schemas-recipeview.md)); title="Recipes" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field recipes](#s-d36c79e332) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field catalog_sha256](#s-701d54ed95) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [RecipeView](schemas-recipeview.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1fb91730df"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-43960d4528"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-3699189329"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeCatalogView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d6fcb9e5527342a1d384115f206f2bf6257a2e8a7ed0d10afae020d23a4fa667 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "catalog_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Catalog Sha256",
      "type": "string"
    },
    "recipes": {
      "items": {
        "$ref": "#/components/schemas/RecipeView"
      },
      "title": "Recipes",
      "type": "array"
    }
  },
  "required": [
    "catalog_sha256",
    "recipes"
  ],
  "title": "RecipeCatalogView",
  "type": "object"
}
```

</details>
