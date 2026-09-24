# schemas: RecipeIdentityRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-recipeidentityref:1b782adf52 -->

Embedded Stove0 reference to the Riverhog recipe identity.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b6ede3d890"></a>

- <a id="s-54137e5325"></a>`type`: `"object"`
- <a id="s-05b6a38bdb"></a>`additionalProperties`: `false`
- <a id="s-f323a92188"></a>`description`: `"Embedded Stove0 reference to the Riverhog recipe identity."`
- <a id="s-62c94f0aa8"></a>`required`: `["id","revision","sha256"]`
- <a id="s-75afc7b778"></a>`title`: `"RecipeIdentityRef"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a397dffbf0"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-1586189874"></a>`revision` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-1b993bc3c6"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-1b993bc3c6) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f0613660fd"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-b38ad90c9b"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeIdentityRef`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30cbf6978037ef16c36e0142aff3ac9933053465ddcbcd1ed2913dd42927be41 -->

```json
{
  "additionalProperties": false,
  "description": "Embedded Stove0 reference to the Riverhog recipe identity.",
  "properties": {
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "revision": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "id",
    "revision",
    "sha256"
  ],
  "title": "RecipeIdentityRef",
  "type": "object"
}
```

</details>
