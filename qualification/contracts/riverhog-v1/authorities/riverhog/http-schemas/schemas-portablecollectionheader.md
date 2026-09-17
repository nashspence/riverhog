# schemas: PortableCollectionHeader

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-portablecollectionheader:b1e613fe18 -->

Bounded immutable metadata that owns one portable file inventory.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b136bcb9c1"></a>

- <a id="s-50d2eb1565"></a>`type`: `"object"`
- <a id="s-157c1fa1c8"></a>`additionalProperties`: `false`
- <a id="s-519f6be1ef"></a>`description`: `"Bounded immutable metadata that owns one portable file inventory."`
- <a id="s-e4ea89c847"></a>`required`: `["collection","content_identity","encryption_format","passphrase_id","provenance_mode"]`
- <a id="s-01256d4246"></a>`title`: `"PortableCollectionHeader"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-24956a7c4e"></a>`collection` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-08d76496f4"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |
| <a id="s-7980aa414c"></a>`encryption_format` | yes | type="string"; minLength=1; title="Encryption Format" |  |
| <a id="s-e67a06239f"></a>`format` | no | type="string"; const="riverhog-collection/v1"; default="riverhog-collection/v1"; title="Format" |  |
| <a id="s-e61a66d8d0"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$"; title="Passphrase Id" |  |
| <a id="s-9d38e0d114"></a>`provenance_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Provenance Identity" |  |
| <a id="s-f354b26fe8"></a>`provenance_mode` | yes | type="string"; enum=["captured","mixed","omitted"]; title="Provenance Mode" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field content_identity](#s-08d76496f4) | `length · characters · fixed` | shared above |
| <a id="s-87c463dd1e"></a>[field provenance_identity · string value](#s-9d38e0d114) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-986ff2fef4"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-dca9d7e94f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PortableCollectionHeader`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bdcd92aa73066da108753249ed0c752b0beeab4e7e77ab187c0a62fc60bfc735 -->

```json
{
  "additionalProperties": false,
  "description": "Bounded immutable metadata that owns one portable file inventory.",
  "properties": {
    "collection": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "content_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Content Identity",
      "type": "string"
    },
    "encryption_format": {
      "minLength": 1,
      "title": "Encryption Format",
      "type": "string"
    },
    "format": {
      "const": "riverhog-collection/v1",
      "default": "riverhog-collection/v1",
      "title": "Format",
      "type": "string"
    },
    "passphrase_id": {
      "pattern": "^[A-Za-z0-9_-]{16,128}$",
      "title": "Passphrase Id",
      "type": "string"
    },
    "provenance_identity": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Provenance Identity"
    },
    "provenance_mode": {
      "enum": [
        "captured",
        "mixed",
        "omitted"
      ],
      "title": "Provenance Mode",
      "type": "string"
    }
  },
  "required": [
    "collection",
    "content_identity",
    "encryption_format",
    "passphrase_id",
    "provenance_mode"
  ],
  "title": "PortableCollectionHeader",
  "type": "object"
}
```

</details>
