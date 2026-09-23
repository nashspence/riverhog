# schemas: OmittedCollectionFileProvenanceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-omittedcollectionfileprovenanceout:9e2a3ac046 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-65bb876d78"></a>

- <a id="s-5c0e8a0452"></a>`type`: `"object"`
- <a id="s-05933d77bd"></a>`additionalProperties`: `false`
- <a id="s-bd5e13f8a3"></a>`required`: `["path","bytes","sha256","collection_id","provenance"]`
- <a id="s-2a0e9ef8da"></a>`title`: `"OmittedCollectionFileProvenanceOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-54be311c32"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-b4234dc0d1"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-314e2a6fc6"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |
| <a id="s-78bb1e70c0"></a>`provenance` | yes | [OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md) |  |
| <a id="s-6377f41a68"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-54be311c32) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-6377f41a68) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CollectionId](schemas-collectionid.md)
- [OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8460693fcf"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-2916578881"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-df17c1ddd7"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OmittedCollectionFileProvenanceOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fdfa74d2d3788b7a0534e4be864dda053c9646ca6c450a6d00024248f6f3235d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
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
  "title": "OmittedCollectionFileProvenanceOut",
  "type": "object"
}
```

</details>
