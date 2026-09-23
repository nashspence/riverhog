# schemas: SearchCollectionsRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-searchcollectionsrequest:586e95f6cd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-00baa3644f"></a>

- <a id="s-ba2f031f2b"></a>`type`: `"object"`
- <a id="s-81c59c19f4"></a>`additionalProperties`: `false`
- <a id="s-9ff5d21c05"></a>`required`: `["tags"]`
- <a id="s-8265685bba"></a>`title`: `"SearchCollectionsRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8e397508fa"></a>`tags` | yes | type="array"; items=([CollectionTag](schemas-collectiontag.md)); maxItems=100; title="Tags"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-tag-selector-batch"} |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; reason="bounded-exact-tag-selector-batch"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field tags](#s-8e397508fa) | `cardinality · items · contract_max` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionTag](schemas-collectiontag.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-064f9c293e"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e3e1ecf8f7"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/SearchCollectionsRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b752ea9a380c065c68ded5efd4b4d7357690352d9a72d0e1c13bcd0bcef816c2 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "tags": {
      "items": {
        "$ref": "#/components/schemas/CollectionTag"
      },
      "maxItems": 100,
      "title": "Tags",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-exact-tag-selector-batch"
      }
    }
  },
  "required": [
    "tags"
  ],
  "title": "SearchCollectionsRequest",
  "type": "object"
}
```

</details>
