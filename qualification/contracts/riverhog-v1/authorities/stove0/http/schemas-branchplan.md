# schemas: BranchPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchplan:1a290228b3 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchPlan`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactSelectionRef](schemas-artifactselectionref.md)
- [schemas: WorkflowPlan](schemas-workflowplan.md)

## Contract summary

- `title`: BranchPlan
- `description`: One named required child work using the ordinary WorkflowPlan contract.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_selection` | yes | #/components/schemas/ArtifactSelectionRef |  |
| `branch_id` | yes | string |  |
| `kind` | no | string |  |
| `workflow_plan` | yes | #/components/schemas/WorkflowPlan |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39986da6d0d0d70636814ba3e4f631cef339fe0c8a681f1f52df62051ce36bc2 -->

```json
{
  "additionalProperties": false,
  "description": "One named required child work using the ordinary WorkflowPlan contract.",
  "properties": {
    "artifact_selection": {
      "$ref": "#/components/schemas/ArtifactSelectionRef"
    },
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "kind": {
      "const": "leaf",
      "default": "leaf",
      "title": "Kind",
      "type": "string"
    },
    "workflow_plan": {
      "$ref": "#/components/schemas/WorkflowPlan"
    }
  },
  "required": [
    "branch_id",
    "artifact_selection",
    "workflow_plan"
  ],
  "title": "BranchPlan",
  "type": "object"
}
```
