# schemas: SchedulerStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-schedulerstatus:7834045647 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: SchedulerStatus
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `interval_seconds` | yes | type="number"; additional keys=`exclusiveMinimum` |  |
| `roles` | yes | type="array"; items=(type="string"; enum=["controller","worker","combined"]) |  |
| `running` | yes | type="boolean" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a4eac0cdfe53ec109a9dd097dd163c7a16e9c52f9f04684ef04035f8d303bd5 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "interval_seconds": {
      "exclusiveMinimum": 0,
      "title": "Interval Seconds",
      "type": "number"
    },
    "roles": {
      "items": {
        "enum": [
          "controller",
          "worker",
          "combined"
        ],
        "type": "string"
      },
      "title": "Roles",
      "type": "array"
    },
    "running": {
      "title": "Running",
      "type": "boolean"
    }
  },
  "required": [
    "running",
    "interval_seconds",
    "roles"
  ],
  "title": "SchedulerStatus",
  "type": "object"
}
```
