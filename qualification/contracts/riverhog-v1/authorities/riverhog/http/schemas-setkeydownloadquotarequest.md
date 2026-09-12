# schemas: SetKeyDownloadQuotaRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-setkeydownloadquotarequest:3cfe0f2dbb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: SetKeyDownloadQuotaRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `monthly_bytes` | yes | anyOf=#/components/schemas/MonthlyDownloadQuotaBytes \| type="null" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)

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

- `/external_contract/http_openapi/riverhog/components/schemas/SetKeyDownloadQuotaRequest`

### Exact owned JSON

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
