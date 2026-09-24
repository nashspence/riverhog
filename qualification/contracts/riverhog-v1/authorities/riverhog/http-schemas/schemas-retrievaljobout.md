# schemas: RetrievalJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievaljobout:b17d14a8b4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-ed5a2f6274"></a>

- <a id="s-9ae89f2ba9"></a>`type`: `"object"`
- <a id="s-46fe1e104b"></a>`additionalProperties`: `false`
- <a id="s-468daa761a"></a>`required`: `["id","plan_id","state","plan_etag","created_at","requested_at","restore_requested_at","ready_at","expires_at","completed_at","canceled_at","failure","lease_seconds","restore_policy","requires_restore"]`
- <a id="s-757ab5370c"></a>`title`: `"RetrievalJobOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4a26bd20ca"></a>`canceled_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Canceled At" |  |
| <a id="s-62ab89c460"></a>`completed_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Completed At" |  |
| <a id="s-3969583590"></a>`created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Created At" |  |
| <a id="s-3f1631fe3f"></a>`expires_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Expires At" |  |
| <a id="s-47cbd02588"></a>`failure` | yes | anyOf=[(type="string"; minLength=1); (type="null")]; title="Failure" |  |
| <a id="s-ba2913517e"></a>`id` | yes | type="string"; title="Id" |  |
| <a id="s-4309221e14"></a>`lease_seconds` | yes | type="integer"; title="Lease Seconds" |  |
| <a id="s-7d910b2388"></a>`plan_etag` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Etag" |  |
| <a id="s-b3a272f4b1"></a>`plan_id` | yes | type="string"; title="Plan Id" |  |
| <a id="s-73a1545b22"></a>`ready_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Ready At" |  |
| <a id="s-e2e4bf916b"></a>`requested_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Requested At" |  |
| <a id="s-a1460da5f4"></a>`requires_restore` | yes | type="boolean"; title="Requires Restore" |  |
| <a id="s-a9bf8be7aa"></a>`restore_policy` | yes | type="string"; enum=["allow","never"]; title="Restore Policy" |  |
| <a id="s-4e2d281293"></a>`restore_requested_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Restore Requested At" |  |
| <a id="s-89d57750d5"></a>`state` | yes | type="string"; enum=["requested","ready","completed","expired","failed","canceled"]; title="State" |  |

### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-b6440e4ca0"></a>1 | properties={state: (const="completed")} | properties={completed_at: (type="string")} | properties={completed_at: (type="null")} |
| <a id="s-04ad3a2fdf"></a>2 | properties={state: (const="canceled")} | properties={canceled_at: (type="string")} | properties={canceled_at: (type="null")} |
| <a id="s-da08214472"></a>3 | properties={state: (const="failed")} | properties={failure: (type="string"; minLength=1)} | no additional constraint |
| <a id="s-51d0a5038f"></a>4 | properties={state: (enum=["requested","failed"])} | no additional constraint | properties={failure: (type="null")} |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-e974e95be5"></a>[field canceled_at · string value](#s-4a26bd20ca) | `length · characters · fixed` | maximum=30; minimum=30 |
| <a id="s-891627f529"></a>[field completed_at · string value](#s-62ab89c460) | `length · characters · fixed` | maximum=30; minimum=30 |
| [field created_at](#s-3969583590) | `length · characters · fixed` | maximum=30; minimum=30 |
| <a id="s-54edeb8854"></a>[field expires_at · string value](#s-3f1631fe3f) | `length · characters · fixed` | maximum=30; minimum=30 |
| [field plan_etag](#s-7d910b2388) | `length · characters · fixed` | maximum=64; minimum=64; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-623efa1136"></a>[field ready_at · string value](#s-73a1545b22) | `length · characters · fixed` | maximum=30; minimum=30 |
| <a id="s-6561b6a15d"></a>[field requested_at · string value](#s-e2e4bf916b) | `length · characters · fixed` | maximum=30; minimum=30 |
| <a id="s-41a80b1e21"></a>[field restore_requested_at · string value](#s-4e2d281293) | `length · characters · fixed` | maximum=30; minimum=30 |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-0336568bc1"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-034f3f9955"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalJobOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e28a3f3e5c63fce74d708846df2e305461913bc38c38e21195b46d97c5c8359d -->

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
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Completed At"
    },
    "created_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Created At",
      "type": "string"
    },
    "expires_at": {
      "anyOf": [
        {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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

</details>
