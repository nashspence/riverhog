# schemas: PortableCollectionInventoryAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-portablecollectioninventoryauthority:df9243ffe3 -->

The immutable authority shared by every bounded inventory page.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: PortableCollectionInventoryAuthority
- `description`: The immutable authority shared by every bounded inventory page.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `file_bytes` | yes | type="integer"; minimum=0 |  |
| `file_count` | yes | type="integer"; minimum=1 |  |
| `header` | yes | #/components/schemas/PortableCollectionHeader |  |
| `inventory_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: PortableCollectionHeader](schemas-portablecollectionheader.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PortableCollectionInventoryAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98ff10220bdc7d51fc7ea37e5234aa18214324afa3fdcb4155f079fed05bc4cb -->

```json
{
  "additionalProperties": false,
  "description": "The immutable authority shared by every bounded inventory page.",
  "properties": {
    "file_bytes": {
      "minimum": 0,
      "title": "File Bytes",
      "type": "integer"
    },
    "file_count": {
      "minimum": 1,
      "title": "File Count",
      "type": "integer"
    },
    "header": {
      "$ref": "#/components/schemas/PortableCollectionHeader"
    },
    "inventory_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Inventory Identity",
      "type": "string"
    }
  },
  "required": [
    "header",
    "inventory_identity",
    "file_count",
    "file_bytes"
  ],
  "title": "PortableCollectionInventoryAuthority",
  "type": "object"
}
```
