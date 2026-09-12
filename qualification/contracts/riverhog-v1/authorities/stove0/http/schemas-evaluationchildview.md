# schemas: EvaluationChildView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationchildview:beddb1d88d -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationChildView`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: OutputCollectionRef](schemas-outputcollectionref.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: EvaluationChildView
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `output` | no | object (1 fields) |  |
| `state` | yes | string |  |
| `variant_id` | yes | string |  |
| `work_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1092b50b2f3a5e988601a981685280dc7968d0b009e510be37e11e1b87eaab9d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "output": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/OutputCollectionRef"
        },
        {
          "type": "null"
        }
      ]
    },
    "state": {
      "enum": [
        "pending",
        "active",
        "complete",
        "inapplicable",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    },
    "variant_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Variant Id",
      "type": "string"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "variant_id",
    "work_id",
    "state"
  ],
  "title": "EvaluationChildView",
  "type": "object"
}
```
