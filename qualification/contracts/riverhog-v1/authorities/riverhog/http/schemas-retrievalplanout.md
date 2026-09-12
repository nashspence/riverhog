# schemas: RetrievalPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalplanout:d94f1ee78a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=10000, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: RetrievalPlanOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `created_at` | yes | string |  |
| `etag` | yes | object (2 fields) |  |
| `expires_at` | yes | string |  |
| `failure` | yes | object (2 fields) |  |
| `file_count` | yes | integer |  |
| `format` | yes | string |  |
| `id` | yes | string |  |
| `lease_seconds` | yes | integer |  |
| `ready_at` | yes | object (2 fields) |  |
| `requires_restore` | yes | boolean |  |
| `restore_policy` | yes | string |  |
| `state` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bfd67616847edef1e39584fbb8604ff423dc38761470242a99f5a66d86f28123 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "created_at": {
      "title": "Created At",
      "type": "string"
    },
    "etag": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Etag"
    },
    "expires_at": {
      "title": "Expires At",
      "type": "string"
    },
    "failure": {
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Failure"
    },
    "file_count": {
      "maximum": 10000,
      "minimum": 1,
      "title": "File Count",
      "type": "integer"
    },
    "format": {
      "const": "riverhog-retrieval-plan/v1",
      "title": "Format",
      "type": "string"
    },
    "id": {
      "title": "Id",
      "type": "string"
    },
    "lease_seconds": {
      "title": "Lease Seconds",
      "type": "integer"
    },
    "ready_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Ready At"
    },
    "requires_restore": {
      "title": "Requires Restore",
      "type": "boolean"
    },
    "restore_policy": {
      "enum": [
        "allow",
        "never"
      ],
      "title": "Restore Policy",
      "type": "string"
    },
    "state": {
      "enum": [
        "planning",
        "ready",
        "consumed",
        "expired",
        "failed"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "format",
    "id",
    "state",
    "created_at",
    "ready_at",
    "expires_at",
    "failure",
    "lease_seconds",
    "restore_policy",
    "requires_restore",
    "file_count",
    "etag"
  ],
  "title": "RetrievalPlanOut",
  "type": "object"
}
```
