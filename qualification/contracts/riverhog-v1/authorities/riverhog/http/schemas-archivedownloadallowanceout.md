# schemas: ArchiveDownloadAllowanceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivedownloadallowanceout:7189efe81a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: ArchiveDownloadAllowanceOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `accounted_bytes` | yes | type="integer" |  |
| `allowance_bytes` | yes | type="integer" |  |
| `effective_limit_bytes` | yes | type="integer" |  |
| `month_started_at` | yes | type="string" |  |
| `remaining_bytes` | yes | type="integer" |  |
| `reserved_bytes` | yes | type="integer" |  |
| `resets_at` | yes | type="string" |  |
| `safety_buffer_bytes` | yes | type="integer" |  |
| `state` | yes | type="string"; enum=["open","closed"] |  |
| `store` | yes | #/components/schemas/ArchiveStoreName |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)

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

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveDownloadAllowanceOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6bf5085d23cac01726e33f1c0aaab7af69c8b4af270cffb72c5387ff400b329a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "accounted_bytes": {
      "title": "Accounted Bytes",
      "type": "integer"
    },
    "allowance_bytes": {
      "title": "Allowance Bytes",
      "type": "integer"
    },
    "effective_limit_bytes": {
      "title": "Effective Limit Bytes",
      "type": "integer"
    },
    "month_started_at": {
      "title": "Month Started At",
      "type": "string"
    },
    "remaining_bytes": {
      "title": "Remaining Bytes",
      "type": "integer"
    },
    "reserved_bytes": {
      "title": "Reserved Bytes",
      "type": "integer"
    },
    "resets_at": {
      "title": "Resets At",
      "type": "string"
    },
    "safety_buffer_bytes": {
      "title": "Safety Buffer Bytes",
      "type": "integer"
    },
    "state": {
      "enum": [
        "open",
        "closed"
      ],
      "title": "State",
      "type": "string"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    }
  },
  "required": [
    "store",
    "state",
    "month_started_at",
    "resets_at",
    "allowance_bytes",
    "safety_buffer_bytes",
    "effective_limit_bytes",
    "accounted_bytes",
    "reserved_bytes",
    "remaining_bytes"
  ],
  "title": "ArchiveDownloadAllowanceOut",
  "type": "object"
}
```
