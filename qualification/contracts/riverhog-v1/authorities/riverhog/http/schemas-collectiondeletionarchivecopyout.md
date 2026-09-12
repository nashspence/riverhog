# schemas: CollectionDeletionArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondeletionarchivecopyout:1d22347208 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletionArchiveCopyOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)

## Contract summary

- `title`: CollectionDeletionArchiveCopyOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `objects` | yes | integer |  |
| `store` | yes | #/components/schemas/ArchiveStoreName |  |
| `stored_bytes` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42d4bdcc75153940ccad496bbce0aa33749ab018e46dded25dddf95544b3db51 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "objects": {
      "title": "Objects",
      "type": "integer"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "stored_bytes": {
      "title": "Stored Bytes",
      "type": "integer"
    }
  },
  "required": [
    "store",
    "objects",
    "stored_bytes"
  ],
  "title": "CollectionDeletionArchiveCopyOut",
  "type": "object"
}
```
