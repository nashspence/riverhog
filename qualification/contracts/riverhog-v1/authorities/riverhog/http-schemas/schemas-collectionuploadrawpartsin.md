# schemas: CollectionUploadRawPartsIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadrawpartsin:896a2a376a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-c3dd20ed1a"></a>

- <a id="s-1fb2218631"></a>`type`: `"object"`
- <a id="s-0306495919"></a>`additionalProperties`: `false`
- <a id="s-c19a15ecf2"></a>`required`: `["part_plaintext_bytes","part_count","ordered_sha256"]`
- <a id="s-d2abbc926c"></a>`title`: `"CollectionUploadRawPartsIn"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-63f2f80203"></a>`ordered_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Ordered Sha256" |  |
| <a id="s-f5f8f9f00b"></a>`part_count` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-ad73d85dc2"></a>`part_plaintext_bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=65536 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field ordered_sha256](#s-63f2f80203) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2a8e86c8fe"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-d7220d076d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadRawPartsIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 189e7d45dcf1cd06ec8078e560f8c5d9410eb17b8fe427808a820503bb5147cd -->

```json
{
  "additionalProperties": false,
  "properties": {
    "ordered_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Ordered Sha256",
      "type": "string"
    },
    "part_count": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "part_plaintext_bytes": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 65536
    }
  },
  "required": [
    "part_plaintext_bytes",
    "part_count",
    "ordered_sha256"
  ],
  "title": "CollectionUploadRawPartsIn",
  "type": "object"
}
```

</details>
