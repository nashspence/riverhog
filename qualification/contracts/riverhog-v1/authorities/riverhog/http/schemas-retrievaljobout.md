# schemas: RetrievalJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievaljobout:77f19d3812 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: RetrievalJobOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `canceled_at` | yes | anyOf=type="string" \| type="null" |  |
| `completed_at` | yes | anyOf=type="string" \| type="null" |  |
| `created_at` | yes | type="string" |  |
| `expires_at` | yes | anyOf=type="string" \| type="null" |  |
| `failure` | yes | anyOf=type="string"; minLength=1 \| type="null" |  |
| `id` | yes | type="string" |  |
| `lease_seconds` | yes | type="integer" |  |
| `plan_etag` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `plan_id` | yes | type="string" |  |
| `ready_at` | yes | anyOf=type="string" \| type="null" |  |
| `requested_at` | yes | anyOf=type="string" \| type="null" |  |
| `requires_restore` | yes | type="boolean" |  |
| `restore_policy` | yes | type="string"; enum=["allow","never"] |  |
| `restore_requested_at` | yes | anyOf=type="string" \| type="null" |  |
| `state` | yes | type="string"; enum=["requested","ready","completed","expired","failed","canceled"] |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

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

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalJobOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d0f00eafedd9f6bb74bd870b5ae7b2767cb28b8f7f7ce704d3ce519d011aeca -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "else": {
        "properties": {
          "completed_at": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "const": "completed"
          }
        }
      },
      "then": {
        "properties": {
          "completed_at": {
            "type": "string"
          }
        }
      }
    },
    {
      "else": {
        "properties": {
          "canceled_at": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "const": "canceled"
          }
        }
      },
      "then": {
        "properties": {
          "canceled_at": {
            "type": "string"
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "state": {
            "const": "failed"
          }
        }
      },
      "then": {
        "properties": {
          "failure": {
            "minLength": 1,
            "type": "string"
          }
        }
      }
    },
    {
      "else": {
        "properties": {
          "failure": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "enum": [
              "requested",
              "failed"
            ]
          }
        }
      }
    }
  ],
  "properties": {
    "canceled_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Canceled At"
    },
    "completed_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Completed At"
    },
    "created_at": {
      "title": "Created At",
      "type": "string"
    },
    "expires_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Expires At"
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
    "id": {
      "title": "Id",
      "type": "string"
    },
    "lease_seconds": {
      "title": "Lease Seconds",
      "type": "integer"
    },
    "plan_etag": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Etag",
      "type": "string"
    },
    "plan_id": {
      "title": "Plan Id",
      "type": "string"
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
    "requested_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Requested At"
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
    "restore_requested_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Restore Requested At"
    },
    "state": {
      "enum": [
        "requested",
        "ready",
        "completed",
        "expired",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "id",
    "plan_id",
    "state",
    "plan_etag",
    "created_at",
    "requested_at",
    "restore_requested_at",
    "ready_at",
    "expires_at",
    "completed_at",
    "canceled_at",
    "failure",
    "lease_seconds",
    "restore_policy",
    "requires_restore"
  ],
  "title": "RetrievalJobOut",
  "type": "object"
}
```
