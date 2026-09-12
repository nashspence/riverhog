# schemas: SchedulerFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-schedulerfailure:ca8e13e6f5 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerFailure`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: SchedulerFailure
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `error` | yes | string |  |
| `event_id` | no | object (2 fields) |  |
| `work_id` | no | object (2 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dff37a74ff588a433e131221fee37f28f528269756151350ad46117bf156245c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "error": {
      "maxLength": 1000,
      "minLength": 1,
      "title": "Error",
      "type": "string"
    },
    "event_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Event Id"
    },
    "work_id": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Work Id"
    }
  },
  "required": [
    "error"
  ],
  "title": "SchedulerFailure",
  "type": "object"
}
```
