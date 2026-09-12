# schemas: SchedulerWorkBatch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-schedulerworkbatch:0c29b4c561 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerWorkBatch`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: SchedulerFailure](schemas-schedulerfailure.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: SchedulerWorkBatch
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `cursor` | yes | string |  |
| `failures` | yes | array |  |
| `next_cursor` | yes | string |  |
| `progressed` | yes | array |  |
| `role` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8bc39e7070ed70678d9f49add76a6806022e801c3461554beebb6ae95228e285 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "cursor": {
      "title": "Cursor",
      "type": "string"
    },
    "failures": {
      "items": {
        "$ref": "#/components/schemas/SchedulerFailure"
      },
      "title": "Failures",
      "type": "array"
    },
    "next_cursor": {
      "title": "Next Cursor",
      "type": "string"
    },
    "progressed": {
      "items": {
        "pattern": "^[0-9a-f]{64}$",
        "type": "string"
      },
      "title": "Progressed",
      "type": "array"
    },
    "role": {
      "enum": [
        "controller",
        "worker",
        "combined"
      ],
      "title": "Role",
      "type": "string"
    }
  },
  "required": [
    "role",
    "cursor",
    "next_cursor",
    "progressed",
    "failures"
  ],
  "title": "SchedulerWorkBatch",
  "type": "object"
}
```
