# schemas: CollectionUploadListFiltersOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadlistfiltersout:b245bb0b24 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadListFiltersOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionUploadState](schemas-collectionuploadstate.md)

## Contract summary

- `title`: CollectionUploadListFiltersOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `state` | yes | object (1 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2367deaea2a301a7886ae46cf9df477060428a27b716384f27d61e80cd32b82c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "state": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionUploadState"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "state"
  ],
  "title": "CollectionUploadListFiltersOut",
  "type": "object"
}
```
