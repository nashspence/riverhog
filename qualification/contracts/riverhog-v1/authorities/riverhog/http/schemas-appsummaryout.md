# schemas: AppSummaryOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appsummaryout:d221fc0283 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppSummaryOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ApplicationName](schemas-applicationname.md)

## Contract summary

- `title`: AppSummaryOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `active_keys` | yes | integer |  |
| `keys` | yes | integer |  |
| `last_used_at` | yes | object (2 fields) |  |
| `name` | yes | #/components/schemas/ApplicationName |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a648910ef6f83e51e08d9cec0bcc14ba96ab319b0811a1203f9260ac13a404cb -->

```json
{
  "additionalProperties": false,
  "properties": {
    "active_keys": {
      "title": "Active Keys",
      "type": "integer"
    },
    "keys": {
      "title": "Keys",
      "type": "integer"
    },
    "last_used_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Last Used At"
    },
    "name": {
      "$ref": "#/components/schemas/ApplicationName"
    }
  },
  "required": [
    "name",
    "keys",
    "active_keys",
    "last_used_at"
  ],
  "title": "AppSummaryOut",
  "type": "object"
}
```
