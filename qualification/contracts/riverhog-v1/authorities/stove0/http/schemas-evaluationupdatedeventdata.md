# schemas: EvaluationUpdatedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationupdatedeventdata:4c4cf43ce6 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationUpdatedEventData`

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
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: EvaluationUpdatedEventData
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `evaluation_id` | yes | string |  |
| `phase` | yes | string |  |
| `revision` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 564ef285d77d3c9dad1bf60136e69ab8f2b535cfdc729c7830aa6ecdb46fd671 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "evaluation_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Evaluation Id",
      "type": "string"
    },
    "phase": {
      "enum": [
        "planning",
        "running",
        "partially_complete",
        "complete",
        "failed",
        "canceled"
      ],
      "title": "Phase",
      "type": "string"
    },
    "revision": {
      "minimum": 2,
      "title": "Revision",
      "type": "integer"
    }
  },
  "required": [
    "evaluation_id",
    "phase",
    "revision"
  ],
  "title": "EvaluationUpdatedEventData",
  "type": "object"
}
```
