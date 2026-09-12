# schemas: SchedulerPruning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-schedulerpruning:55856f9bcc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: SchedulerPruning
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `evaluation_bytes` | yes | type="integer"; minimum=0 |  |
| `evaluations` | yes | type="integer"; minimum=0 |  |
| `event_bytes` | yes | type="integer"; minimum=0 |  |
| `events` | yes | type="integer"; minimum=0 |  |
| `selection_bytes` | yes | type="integer"; minimum=0 |  |
| `selections` | yes | type="integer"; minimum=0 |  |
| `work` | yes | type="integer"; minimum=0 |  |
| `work_bytes` | yes | type="integer"; minimum=0 |  |

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerPruning`

### Exact owned JSON

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
