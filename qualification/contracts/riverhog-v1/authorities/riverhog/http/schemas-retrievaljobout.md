# schemas: RetrievalJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievaljobout:77f19d3812 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-ed5a2f6274f2"></a>
- <a id="s-757ab5370c0c"></a>`title`: RetrievalJobOut
- <a id="s-9ae89f2ba90f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4a26bd20caa7"></a>`canceled_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-62ab89c460f9"></a>`completed_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-396958359017"></a>`created_at` | yes | type="string" |  |
| <a id="s-3f1631fe3f17"></a>`expires_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-47cbd0258884"></a>`failure` | yes | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-ba2913517e44"></a>`id` | yes | type="string" |  |
| <a id="s-4309221e1442"></a>`lease_seconds` | yes | type="integer" |  |
| <a id="s-7d910b2388a5"></a>`plan_etag` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b3a272f4b1d6"></a>`plan_id` | yes | type="string" |  |
| <a id="s-73a1545b22b5"></a>`ready_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-e2e4bf916b05"></a>`requested_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-a1460da5f4a2"></a>`requires_restore` | yes | type="boolean" |  |
| <a id="s-a9bf8be7aa67"></a>`restore_policy` | yes | type="string"; enum=["allow","never"] |  |
| <a id="s-4e2d281293d3"></a>`restore_requested_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-89d57750d548"></a>`state` | yes | type="string"; enum=["requested","ready","completed","expired","failed","canceled"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field plan_etag](#s-7d910b2388a5) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-2886b9ab2015"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-b69d58f68454"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
