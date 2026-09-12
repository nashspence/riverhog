# schemas: PortableCollectionHeader

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-portablecollectionheader:c3da8f056c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PortableCollectionHeader`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: PortableCollectionHeader
- `description`: Bounded immutable metadata that owns one portable file inventory.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection` | yes | #/components/schemas/CollectionId |  |
| `content_identity` | yes | string |  |
| `encryption_format` | yes | string |  |
| `format` | no | string |  |
| `passphrase_id` | yes | string |  |
| `provenance_identity` | no | object (2 fields) |  |
| `provenance_mode` | yes | string |  |

## Complete owned contract

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
