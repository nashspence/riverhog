# schemas: SchedulerRunIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-schedulerrunin:18e8ecf285 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerRunIn`

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
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: SchedulerRunIn
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `role` | no | string |  |
| `work_limit` | no | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb6999171d296c228fdb5ff3ba85eb17a54fcbccdc9cfd7f0af37f0db3209dd5 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "role": {
      "default": "combined",
      "enum": [
        "controller",
        "worker",
        "combined"
      ],
      "title": "Role",
      "type": "string"
    },
    "work_limit": {
      "default": 25,
      "maximum": 100,
      "minimum": 1,
      "title": "Work Limit",
      "type": "integer"
    }
  },
  "title": "SchedulerRunIn",
  "type": "object"
}
```
