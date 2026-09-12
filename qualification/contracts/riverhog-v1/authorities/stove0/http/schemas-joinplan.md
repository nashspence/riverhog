# schemas: JoinPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-joinplan:8ee030705d -->

Resolved ordinary join work over exact successful branch outputs.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

- `title`: JoinPlan
- `description`: Resolved ordinary join work over exact successful branch outputs.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `declaration` | yes | #/components/schemas/JoinDeclaration |  |
| `format` | no | type="string"; const="stove0-join-plan/v1" |  |
| `inputs` | yes | type="array"; minItems=2; items=(#/components/schemas/JoinInputPlan) |  |
| `join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `work` | yes | #/components/schemas/WorkIdentity |  |
| `workflow_plan` | yes | #/components/schemas/WorkflowPlan |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JoinDeclaration](schemas-joindeclaration.md)
- [schemas: JoinInputPlan](schemas-joininputplan.md)
- [schemas: WorkIdentity](schemas-workidentity.md)
- [schemas: WorkflowPlan](schemas-workflowplan.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinPlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1b6d7c37f18315c7589dd9d1189f081c4c0bb74d3cb970cf7a630775c436923d -->

```json
{
  "additionalProperties": false,
  "description": "Resolved ordinary join work over exact successful branch outputs.",
  "properties": {
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
      "type": "string"
    },
    "declaration": {
      "$ref": "#/components/schemas/JoinDeclaration"
    },
    "format": {
      "const": "stove0-join-plan/v1",
      "default": "stove0-join-plan/v1",
      "title": "Format",
      "type": "string"
    },
    "inputs": {
      "items": {
        "$ref": "#/components/schemas/JoinInputPlan"
      },
      "minItems": 2,
      "title": "Inputs",
      "type": "array"
    },
    "join_plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Join Plan Sha256",
      "type": "string"
    },
    "parent_work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Parent Work Id",
      "type": "string"
    },
    "work": {
      "$ref": "#/components/schemas/WorkIdentity"
    },
    "workflow_plan": {
      "$ref": "#/components/schemas/WorkflowPlan"
    }
  },
  "required": [
    "parent_work_id",
    "branch_set_sha256",
    "declaration",
    "inputs",
    "work",
    "workflow_plan",
    "join_plan_sha256"
  ],
  "title": "JoinPlan",
  "type": "object"
}
```
