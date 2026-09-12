# schemas: RetrievalPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalplanout:d94f1ee78a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: RetrievalPlanOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `created_at` | yes | type="string" |  |
| `etag` | yes | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| `expires_at` | yes | type="string" |  |
| `failure` | yes | anyOf=type="string"; minLength=1 \| type="null" |  |
| `file_count` | yes | type="integer"; minimum=1; maximum=10000 |  |
| `format` | yes | type="string"; const="riverhog-retrieval-plan/v1" |  |
| `id` | yes | type="string" |  |
| `lease_seconds` | yes | type="integer" |  |
| `ready_at` | yes | anyOf=type="string" \| type="null" |  |
| `requires_restore` | yes | type="boolean" |  |
| `restore_policy` | yes | type="string"; enum=["allow","never"] |  |
| `state` | yes | type="string"; enum=["planning","ready","consumed","expired","failed"] |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=10000, minimum=1, reason=schema-maximum |

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanOut`

### Exact owned JSON

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
