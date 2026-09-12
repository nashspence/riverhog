# schemas: ExecutionEnvelope

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-executionenvelope:610b04b926 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ExecutionEnvelope`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: TargetPlanBinding](schemas-targetplanbinding.md)
- [schemas: WorkflowPlan](schemas-workflowplan.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ExecutionEnvelope
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `claim_id` | yes | string |  |
| `execution_envelope_sha256` | yes | string |  |
| `fence` | yes | integer |  |
| `format` | no | string |  |
| `target_plan` | yes | #/components/schemas/TargetPlanBinding |  |
| `workflow_plan` | yes | #/components/schemas/WorkflowPlan |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4fb5bd658e35492c990cbae657cbfe2c6bc098a5f9346c50a331ebc112d3ae41 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "claim_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Claim Id",
      "type": "string"
    },
    "execution_envelope_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Execution Envelope Sha256",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "format": {
      "const": "stove0-execution-envelope/v1",
      "default": "stove0-execution-envelope/v1",
      "title": "Format",
      "type": "string"
    },
    "target_plan": {
      "$ref": "#/components/schemas/TargetPlanBinding"
    },
    "workflow_plan": {
      "$ref": "#/components/schemas/WorkflowPlan"
    }
  },
  "required": [
    "claim_id",
    "fence",
    "workflow_plan",
    "target_plan",
    "execution_envelope_sha256"
  ],
  "title": "ExecutionEnvelope",
  "type": "object"
}
```
