# schemas: WorkflowPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workflowplan:8ac5e095fa -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkflowPlan`

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
- [schemas: ObservationEvidence](schemas-observationevidence.md)
- [schemas: OperationRef](schemas-operationref.md)
- [schemas: WorkIdentity](schemas-workidentity.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: WorkflowPlan
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `format` | no | string |  |
| `input_retrieval_policy` | no | string |  |
| `observations` | no | array |  |
| `operation` | yes | #/components/schemas/OperationRef |  |
| `output_policy` | no | object |  |
| `requested_target_options` | no | object |  |
| `result_kind` | no | string |  |
| `retirement_grace_seconds` | no | integer |  |
| `retirement_policy` | no | string |  |
| `target_contract_sha256` | yes | string |  |
| `target_registration_id` | yes | string |  |
| `work` | yes | #/components/schemas/WorkIdentity |  |
| `workflow_plan_sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1ac28da8e579dbffb0ead200dbc696f5253dae3438526682093a5f3e85c9c7bf -->

```json
{
  "additionalProperties": false,
  "properties": {
    "format": {
      "const": "stove0-workflow-plan/v1",
      "default": "stove0-workflow-plan/v1",
      "title": "Format",
      "type": "string"
    },
    "input_retrieval_policy": {
      "default": "available-only",
      "enum": [
        "available-only",
        "allow"
      ],
      "title": "Input Retrieval Policy",
      "type": "string"
    },
    "observations": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/ObservationEvidence"
      },
      "title": "Observations",
      "type": "array"
    },
    "operation": {
      "$ref": "#/components/schemas/OperationRef"
    },
    "output_policy": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Output Policy",
      "type": "object"
    },
    "requested_target_options": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Requested Target Options",
      "type": "object"
    },
    "result_kind": {
      "default": "collection",
      "enum": [
        "collection",
        "external-effect"
      ],
      "title": "Result Kind",
      "type": "string"
    },
    "retirement_grace_seconds": {
      "default": 0,
      "minimum": 0,
      "title": "Retirement Grace Seconds",
      "type": "integer"
    },
    "retirement_policy": {
      "default": "retain",
      "enum": [
        "retain",
        "retire-after-verified-output"
      ],
      "title": "Retirement Policy",
      "type": "string"
    },
    "target_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Target Contract Sha256",
      "type": "string"
    },
    "target_registration_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
      "title": "Target Registration Id",
      "type": "string"
    },
    "work": {
      "$ref": "#/components/schemas/WorkIdentity"
    },
    "workflow_plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Workflow Plan Sha256",
      "type": "string"
    }
  },
  "required": [
    "work",
    "operation",
    "target_registration_id",
    "target_contract_sha256",
    "workflow_plan_sha256"
  ],
  "title": "WorkflowPlan",
  "type": "object"
}
```
