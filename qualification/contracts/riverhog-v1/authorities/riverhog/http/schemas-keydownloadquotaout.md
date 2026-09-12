# schemas: KeyDownloadQuotaOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-keydownloadquotaout:eccde43198 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/KeyDownloadQuotaOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ApplicationKeyId](schemas-applicationkeyid.md)
- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)

## Contract summary

- `title`: KeyDownloadQuotaOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `accounted_bytes` | yes | integer |  |
| `app` | yes | #/components/schemas/ApplicationName |  |
| `id` | yes | string |  |
| `key_id` | yes | #/components/schemas/ApplicationKeyId |  |
| `key_status` | yes | string |  |
| `month_started_at` | yes | string |  |
| `monthly_bytes` | yes | object (1 fields) |  |
| `remaining_bytes` | yes | object (2 fields) |  |
| `reserved_bytes` | yes | integer |  |
| `resets_at` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61974ed36a10ed767b51cd2ba12717626346883603fb66eb7d4c249484173525 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "accounted_bytes": {
      "minimum": 0,
      "title": "Accounted Bytes",
      "type": "integer"
    },
    "app": {
      "$ref": "#/components/schemas/ApplicationName"
    },
    "id": {
      "title": "Id",
      "type": "string"
    },
    "key_id": {
      "$ref": "#/components/schemas/ApplicationKeyId"
    },
    "key_status": {
      "enum": [
        "active",
        "expired",
        "revoked"
      ],
      "title": "Key Status",
      "type": "string"
    },
    "month_started_at": {
      "title": "Month Started At",
      "type": "string"
    },
    "monthly_bytes": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/MonthlyDownloadQuotaBytes"
        },
        {
          "type": "null"
        }
      ]
    },
    "remaining_bytes": {
      "anyOf": [
        {
          "minimum": 0,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Remaining Bytes"
    },
    "reserved_bytes": {
      "minimum": 0,
      "title": "Reserved Bytes",
      "type": "integer"
    },
    "resets_at": {
      "title": "Resets At",
      "type": "string"
    }
  },
  "required": [
    "id",
    "app",
    "key_id",
    "key_status",
    "monthly_bytes",
    "month_started_at",
    "resets_at",
    "accounted_bytes",
    "reserved_bytes",
    "remaining_bytes"
  ],
  "title": "KeyDownloadQuotaOut",
  "type": "object"
}
```
