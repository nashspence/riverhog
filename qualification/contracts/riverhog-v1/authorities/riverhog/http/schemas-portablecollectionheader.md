# schemas: PortableCollectionHeader

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-portablecollectionheader:c3da8f056c -->

Bounded immutable metadata that owns one portable file inventory.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-b136bcb9c1"></a>
- <a id="s-01256d4246"></a>`title`: PortableCollectionHeader
- <a id="s-519f6be1ef"></a>`description`: Bounded immutable metadata that owns one portable file inventory.
- <a id="s-50d2eb1565"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-24956a7c4e"></a>`collection` | yes | #/components/schemas/CollectionId |  |
| <a id="s-08d76496f4"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7980aa414c"></a>`encryption_format` | yes | type="string"; minLength=1 |  |
| <a id="s-e67a06239f"></a>`format` | no | type="string"; const="riverhog-collection/v1" |  |
| <a id="s-e61a66d8d0"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$" |  |
| <a id="s-9d38e0d114"></a>`provenance_identity` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-f354b26fe8"></a>`provenance_mode` | yes | type="string"; enum=["captured","mixed","omitted"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field content_identity](#s-08d76496f4) | `length · characters · fixed` | shared above |
| <a id="s-87c463dd1e"></a>[field provenance_identity · string value](#s-9d38e0d114) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-e6cd81f9dd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-06450f4f14"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PortableCollectionHeader`

### Exact owned JSON

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
