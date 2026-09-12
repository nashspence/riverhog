# schemas: ArchiveCopyJobListFiltersOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyjoblistfiltersout:e8738e9197 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobListFiltersOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArchiveCopyState](schemas-archivecopystate.md)

## Contract summary

- `title`: ArchiveCopyJobListFiltersOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `state` | no | object (1 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ffadbfe6a984f8ff781319075ed287f5e4ad500921415287dad46c363a38e567 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "state": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArchiveCopyState"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "title": "ArchiveCopyJobListFiltersOut",
  "type": "object"
}
```
