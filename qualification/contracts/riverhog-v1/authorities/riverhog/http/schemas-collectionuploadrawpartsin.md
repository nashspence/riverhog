# schemas: CollectionUploadRawPartsIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadrawpartsin:6bc3d512dc -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadRawPartsIn`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: CollectionUploadRawPartsIn
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `ordered_sha256` | yes | string |  |
| `part_count` | yes | integer |  |
| `part_plaintext_bytes` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 76ccb435180670cd87a5f21b23e35a4dfbc75f28f4535248bc6671c05f6a8baf -->

```json
{
  "additionalProperties": false,
  "properties": {
    "ordered_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Ordered Sha256",
      "type": "string"
    },
    "part_count": {
      "minimum": 1,
      "title": "Part Count",
      "type": "integer"
    },
    "part_plaintext_bytes": {
      "minimum": 65536,
      "title": "Part Plaintext Bytes",
      "type": "integer"
    }
  },
  "required": [
    "part_plaintext_bytes",
    "part_count",
    "ordered_sha256"
  ],
  "title": "CollectionUploadRawPartsIn",
  "type": "object"
}
```
