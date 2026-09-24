# schemas: CollectionUploadArtifactCustodyReceiptDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadartifactcustodyre-65e2ca0901:97b6087ccf -->

Exact safe-release evidence for one artifact in construction state.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-de01edc6c7"></a>

- <a id="s-7ba375dfb5"></a>`type`: `"object"`
- <a id="s-7c2724ec4d"></a>`additionalProperties`: `false`
- <a id="s-881a8cdfbb"></a>`description`: `"Exact safe-release evidence for one artifact in construction state."`
- <a id="s-3acd69ba7d"></a>`required`: `["collection_id","path","bytes","sha256","archive_object_count","archive_object_set_sha256","receipt_sha256"]`
- <a id="s-991238a92b"></a>`title`: `"CollectionUploadArtifactCustodyReceiptDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a0567d04d6"></a>`archive_object_count` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-652d1bb86c"></a>`archive_object_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Object Set Sha256" |  |
| <a id="s-5ff22aaac2"></a>`bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md) |  |
| <a id="s-48eeb34d30"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-db6cbedc39"></a>`format` | no | type="string"; const="riverhog-artifact-custody-receipt/v1"; default="riverhog-artifact-custody-receipt/v1"; title="Format" |  |
| <a id="s-478a27e2aa"></a>`path` | yes | type="string"; title="Path" |  |
| <a id="s-0ee690ffb9"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Receipt Sha256" |  |
| <a id="s-c54e529e15"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_object_set_sha256](#s-652d1bb86c) | `length · characters · fixed` | shared above |
| [field receipt_sha256](#s-0ee690ffb9) | `length · characters · fixed` | shared above |
| [field sha256](#s-c54e529e15) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-4dcb128901"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-17d33ba70c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadArtifactCustodyReceiptDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95cce9400e0d3b0ecf773ddf324b378868ea1b99dfd2fa40726123cb267fa6e4 -->

```json
{
  "additionalProperties": false,
  "description": "Exact safe-release evidence for one artifact in construction state.",
  "properties": {
    "archive_object_count": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "archive_object_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Archive Object Set Sha256",
      "type": "string"
    },
    "bytes": {
      "$ref": "#/components/schemas/NonnegativeDecimal"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "format": {
      "const": "riverhog-artifact-custody-receipt/v1",
      "default": "riverhog-artifact-custody-receipt/v1",
      "title": "Format",
      "type": "string"
    },
    "path": {
      "title": "Path",
      "type": "string"
    },
    "receipt_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Receipt Sha256",
      "type": "string"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "path",
    "bytes",
    "sha256",
    "archive_object_count",
    "archive_object_set_sha256",
    "receipt_sha256"
  ],
  "title": "CollectionUploadArtifactCustodyReceiptDocument",
  "type": "object"
}
```

</details>
