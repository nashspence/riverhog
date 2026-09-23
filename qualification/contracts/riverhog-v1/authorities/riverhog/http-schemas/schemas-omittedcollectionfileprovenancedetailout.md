# schemas: OmittedCollectionFileProvenanceDetailOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-omittedcollectionfileprovenancedetailout:1b31f1d072 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-6a1d90839e"></a>

- <a id="s-8bef67ed1f"></a>`type`: `"object"`
- <a id="s-ac1b60b85f"></a>`additionalProperties`: `false`
- <a id="s-eb5cfa73ab"></a>`required`: `["path","bytes","sha256","collection_id","provenance"]`
- <a id="s-16d17d454b"></a>`title`: `"OmittedCollectionFileProvenanceDetailOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-74bef0d03f"></a>`bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md) |  |
| <a id="s-790268a317"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-b55f5e9ca6"></a>`journal` | no | type="null"; title="Journal" |  |
| <a id="s-2fd78d97cb"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |
| <a id="s-541bdc4527"></a>`provenance` | yes | [OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md) |  |
| <a id="s-12666fb0d1"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-12666fb0d1) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CollectionId](schemas-collectionid.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)
- [OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2f76d90c3d"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-4aad6c3d35"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OmittedCollectionFileProvenanceDetailOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0927ea35d7c4ef3221a311b91993bf25a6e378f1060bfe58eb87477a31ae4f01 -->

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
    "journal": {
      "title": "Journal",
      "type": "null"
    },
    "path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    },
    "provenance": {
      "$ref": "#/components/schemas/OmittedFileProvenanceBinding"
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
  "title": "OmittedCollectionFileProvenanceDetailOut",
  "type": "object"
}
```

</details>
