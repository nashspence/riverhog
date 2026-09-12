# schemas: SetKeyDownloadQuotaRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-setkeydownloadquotarequest:3cfe0f2dbb -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/SetKeyDownloadQuotaRequest`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)

## Contract summary

- `title`: SetKeyDownloadQuotaRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `monthly_bytes` | yes | object (1 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5db7dca3d11b00250fcdfb044e4521283e93acdaaece25d0d55ba8256d0bdd24 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "monthly_bytes": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/MonthlyDownloadQuotaBytes"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "monthly_bytes"
  ],
  "title": "SetKeyDownloadQuotaRequest",
  "type": "object"
}
```
