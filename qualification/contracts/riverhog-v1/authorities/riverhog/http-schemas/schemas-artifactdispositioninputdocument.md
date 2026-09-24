# schemas: ArtifactDispositionInputDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-artifactdispositioninputdocument:c38e6513d9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-dbef3c997a"></a>

- <a id="s-0776603369"></a>`type`: `"object"`
- <a id="s-3defca387a"></a>`additionalProperties`: `false`
- <a id="s-bcee5565ba"></a>`required`: `["collection_id","archive_root_sha256","path"]`
- <a id="s-dd8fc78ddc"></a>`title`: `"ArtifactDispositionInputDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cb51991907"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-24be47cb35"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-02bd19cfcd"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_root_sha256](#s-cb51991907) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f1d3632988"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-c6f717c0bf"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionInputDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 18a9d379929f7e070761f6950cd137bfefaf471c2866f75d414b7faec3e731b0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_root_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Archive Root Sha256",
      "type": "string"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    }
  },
  "required": [
    "collection_id",
    "archive_root_sha256",
    "path"
  ],
  "title": "ArtifactDispositionInputDocument",
  "type": "object"
}
```

</details>
