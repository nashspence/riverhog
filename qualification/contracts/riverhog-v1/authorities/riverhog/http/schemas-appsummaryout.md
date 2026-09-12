# schemas: AppSummaryOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appsummaryout:d221fc0283 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: AppSummaryOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `active_keys` | yes | type="integer" |  |
| `keys` | yes | type="integer" |  |
| `last_used_at` | yes | anyOf=type="string" \| type="null" |  |
| `name` | yes | #/components/schemas/ApplicationName |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationName](schemas-applicationname.md)

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppSummaryOut`

### Exact owned JSON

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
