# schemas: CollectionRootIdentityRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-collectionrootidentityref:47aa32793d -->

Embedded Stove0 reference to the Riverhog collection-root identity.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-871a4641f4"></a>

- <a id="s-59f2ffe44b"></a>`type`: `"object"`
- <a id="s-e16b9189c0"></a>`additionalProperties`: `false`
- <a id="s-d8eca6369a"></a>`description`: `"Embedded Stove0 reference to the Riverhog collection-root identity."`
- <a id="s-8b53f2e6aa"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`
- <a id="s-0f7c373bf1"></a>`title`: `"CollectionRootIdentityRef"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-145133d39a"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-402ed5a82f"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-78dd2146b7"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_root_sha256](#s-145133d39a) | `length · characters · fixed` | shared above |
| [field content_identity](#s-78dd2146b7) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6e400a6d24"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-892c5a9c11"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CollectionRootIdentityRef`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3bcc9c461d7f7039d61feb1fbf0d27e0ab60ae2ba3b9198822806cdd61313fa -->

```json
{
  "additionalProperties": false,
  "description": "Embedded Stove0 reference to the Riverhog collection-root identity.",
  "properties": {
    "archive_root_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Archive Root Sha256",
      "type": "string"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "content_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Content Identity",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "archive_root_sha256",
    "content_identity"
  ],
  "title": "CollectionRootIdentityRef",
  "type": "object"
}
```

</details>
