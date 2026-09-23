# schemas: CapturedCollectionFileProvenanceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-capturedcollectionfileprovenanceout:d051f00d07 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-09e0cf4303"></a>

- <a id="s-e474ffa4ad"></a>`type`: `"object"`
- <a id="s-a42f30d011"></a>`additionalProperties`: `false`
- <a id="s-52b738cb11"></a>`required`: `["path","bytes","sha256","collection_id","provenance"]`
- <a id="s-e1a108a3a6"></a>`title`: `"CapturedCollectionFileProvenanceOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ea61dcfe13"></a>`bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md) |  |
| <a id="s-df5b3bbf20"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-0c6f6da560"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |
| <a id="s-f46b091673"></a>`provenance` | yes | [CapturedFileProvenanceBinding](schemas-capturedfileprovenancebinding.md) |  |
| <a id="s-2fea47887d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-2fea47887d) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CapturedFileProvenanceBinding](schemas-capturedfileprovenancebinding.md)
- [CollectionId](schemas-collectionid.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c4d442c140"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-315734372f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CapturedCollectionFileProvenanceOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae7f529cdc6f249ec166880b38bf13a1e9bf6360026ccffda8061a61b2aa5925 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "$ref": "#/components/schemas/NonnegativeDecimal"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    },
    "provenance": {
      "$ref": "#/components/schemas/CapturedFileProvenanceBinding"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "path",
    "bytes",
    "sha256",
    "collection_id",
    "provenance"
  ],
  "title": "CapturedCollectionFileProvenanceOut",
  "type": "object"
}
```

</details>
