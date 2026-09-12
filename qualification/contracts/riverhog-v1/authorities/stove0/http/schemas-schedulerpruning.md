# schemas: SchedulerPruning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-schedulerpruning:55856f9bcc -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerPruning`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `title`: SchedulerPruning
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `evaluation_bytes` | yes | integer |  |
| `evaluations` | yes | integer |  |
| `event_bytes` | yes | integer |  |
| `events` | yes | integer |  |
| `selection_bytes` | yes | integer |  |
| `selections` | yes | integer |  |
| `work` | yes | integer |  |
| `work_bytes` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 639614e3a2443f84fe63c2477757b5c11d526671cd5d37014cf1699993cccc8c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "evaluation_bytes": {
      "minimum": 0,
      "title": "Evaluation Bytes",
      "type": "integer"
    },
    "evaluations": {
      "minimum": 0,
      "title": "Evaluations",
      "type": "integer"
    },
    "event_bytes": {
      "minimum": 0,
      "title": "Event Bytes",
      "type": "integer"
    },
    "events": {
      "minimum": 0,
      "title": "Events",
      "type": "integer"
    },
    "selection_bytes": {
      "minimum": 0,
      "title": "Selection Bytes",
      "type": "integer"
    },
    "selections": {
      "minimum": 0,
      "title": "Selections",
      "type": "integer"
    },
    "work": {
      "minimum": 0,
      "title": "Work",
      "type": "integer"
    },
    "work_bytes": {
      "minimum": 0,
      "title": "Work Bytes",
      "type": "integer"
    }
  },
  "required": [
    "work",
    "work_bytes",
    "evaluations",
    "evaluation_bytes",
    "selections",
    "selection_bytes",
    "events",
    "event_bytes"
  ],
  "title": "SchedulerPruning",
  "type": "object"
}
```
