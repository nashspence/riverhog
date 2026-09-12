# schemas: ArchiveStoreOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivestoreout:b097b51a91 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveStoreOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArchiveDownloadAllowanceOut](schemas-archivedownloadallowanceout.md)
- [schemas: ArchiveStoreName](schemas-archivestorename.md)

## Contract summary

- `title`: ArchiveStoreOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collections` | yes | integer |  |
| `download_allowance` | yes | object (1 fields) |  |
| `objects` | yes | integer |  |
| `read_mode` | yes | string |  |
| `read_priority` | yes | integer |  |
| `store` | yes | #/components/schemas/ArchiveStoreName |  |
| `stored_bytes` | yes | integer |  |
| `write_target` | yes | boolean |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63b4747d492a9117cbf7db30dbe0694eed06b814ae9ed0648d2808cffe495d8e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collections": {
      "title": "Collections",
      "type": "integer"
    },
    "download_allowance": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArchiveDownloadAllowanceOut"
        },
        {
          "type": "null"
        }
      ]
    },
    "objects": {
      "title": "Objects",
      "type": "integer"
    },
    "read_mode": {
      "enum": [
        "immediate",
        "restore_required"
      ],
      "title": "Read Mode",
      "type": "string"
    },
    "read_priority": {
      "title": "Read Priority",
      "type": "integer"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "stored_bytes": {
      "title": "Stored Bytes",
      "type": "integer"
    },
    "write_target": {
      "title": "Write Target",
      "type": "boolean"
    }
  },
  "required": [
    "store",
    "read_mode",
    "read_priority",
    "write_target",
    "collections",
    "objects",
    "stored_bytes",
    "download_allowance"
  ],
  "title": "ArchiveStoreOut",
  "type": "object"
}
```
