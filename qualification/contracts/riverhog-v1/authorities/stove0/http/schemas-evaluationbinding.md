# schemas: EvaluationBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationbinding:ac7f75d74c -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationBinding`

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

- [schemas: JsonValue](schemas-jsonvalue.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: EvaluationBinding
- `description`: Immutable membership of one work item in a trial/evaluation matrix.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `evaluation_id` | yes | string |  |
| `matrix_sha256` | yes | string |  |
| `parameters` | no | object |  |
| `variant_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7e2445a55677db47a8bf6815b4e0bdea90186cd0fa199e2761b789fabc3c17be -->

```json
{
  "additionalProperties": false,
  "description": "Immutable membership of one work item in a trial/evaluation matrix.",
  "properties": {
    "evaluation_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Evaluation Id",
      "type": "string"
    },
    "matrix_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Matrix Sha256",
      "type": "string"
    },
    "parameters": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Parameters",
      "type": "object"
    },
    "variant_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Variant Id",
      "type": "string"
    }
  },
  "required": [
    "evaluation_id",
    "matrix_sha256",
    "variant_id"
  ],
  "title": "EvaluationBinding",
  "type": "object"
}
```
