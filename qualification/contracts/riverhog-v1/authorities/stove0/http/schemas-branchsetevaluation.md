# schemas: BranchSetEvaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchsetevaluation:7f019b5895 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 11 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetEvaluation`

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

- [schemas: BranchEffectSettlement](schemas-brancheffectsettlement.md)
- [schemas: BranchSettlement](schemas-branchsettlement.md)
- [schemas: CoordinationSettlement](schemas-coordinationsettlement.md)
- [schemas: JoinPlan](schemas-joinplan.md)
- [schemas: JoinSettlement](schemas-joinsettlement.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: BranchSetEvaluation
- `description`: Entirely derived view over a plan and ordinary child/join results.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `branch_set_sha256` | yes | string |  |
| `branch_set_succeeded` | yes | boolean |  |
| `canceled_branch_ids` | yes | array |  |
| `coordination_complete_for_retirement` | yes | boolean |  |
| `coordination_settlement` | yes | object (1 fields) |  |
| `failed_branch_ids` | yes | array |  |
| `inapplicable_branch_ids` | yes | array |  |
| `interrupted_branch_ids` | yes | array |  |
| `join_ready` | yes | boolean |  |
| `join_settlement` | yes | object (1 fields) |  |
| `join_state` | yes | string |  |
| `resolved_join_plan` | yes | object (1 fields) |  |
| `retirement_requested` | yes | boolean |  |
| `succeeded_branches` | yes | array |  |
| `succeeded_coordinations` | yes | array |  |
| `succeeded_effects` | yes | array |  |
| `unsettled_branch_ids` | yes | array |  |
| `unsettled_work_ids` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7d5ac96162389e5952fc1f2a7b3aa3f03379bd18a10fbbc20ee185cdbde36b2 -->

```json
{
  "additionalProperties": false,
  "description": "Entirely derived view over a plan and ordinary child/join results.",
  "properties": {
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
      "type": "string"
    },
    "branch_set_succeeded": {
      "title": "Branch Set Succeeded",
      "type": "boolean"
    },
    "canceled_branch_ids": {
      "items": {
        "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
        "type": "string"
      },
      "title": "Canceled Branch Ids",
      "type": "array"
    },
    "coordination_complete_for_retirement": {
      "title": "Coordination Complete For Retirement",
      "type": "boolean"
    },
    "coordination_settlement": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CoordinationSettlement"
        },
        {
          "type": "null"
        }
      ]
    },
    "failed_branch_ids": {
      "items": {
        "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
        "type": "string"
      },
      "title": "Failed Branch Ids",
      "type": "array"
    },
    "inapplicable_branch_ids": {
      "items": {
        "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
        "type": "string"
      },
      "title": "Inapplicable Branch Ids",
      "type": "array"
    },
    "interrupted_branch_ids": {
      "items": {
        "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
        "type": "string"
      },
      "title": "Interrupted Branch Ids",
      "type": "array"
    },
    "join_ready": {
      "title": "Join Ready",
      "type": "boolean"
    },
    "join_settlement": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/JoinSettlement"
        },
        {
          "type": "null"
        }
      ]
    },
    "join_state": {
      "enum": [
        "not-declared",
        "waiting",
        "ready",
        "succeeded",
        "failed",
        "inapplicable",
        "interrupted",
        "canceled"
      ],
      "title": "Join State",
      "type": "string"
    },
    "resolved_join_plan": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/JoinPlan"
        },
        {
          "type": "null"
        }
      ]
    },
    "retirement_requested": {
      "title": "Retirement Requested",
      "type": "boolean"
    },
    "succeeded_branches": {
      "items": {
        "$ref": "#/components/schemas/BranchSettlement"
      },
      "title": "Succeeded Branches",
      "type": "array"
    },
    "succeeded_coordinations": {
      "items": {
        "$ref": "#/components/schemas/CoordinationSettlement"
      },
      "title": "Succeeded Coordinations",
      "type": "array"
    },
    "succeeded_effects": {
      "items": {
        "$ref": "#/components/schemas/BranchEffectSettlement"
      },
      "title": "Succeeded Effects",
      "type": "array"
    },
    "unsettled_branch_ids": {
      "items": {
        "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
        "type": "string"
      },
      "title": "Unsettled Branch Ids",
      "type": "array"
    },
    "unsettled_work_ids": {
      "items": {
        "pattern": "^[0-9a-f]{64}$",
        "type": "string"
      },
      "title": "Unsettled Work Ids",
      "type": "array"
    }
  },
  "required": [
    "branch_set_sha256",
    "succeeded_branches",
    "succeeded_effects",
    "succeeded_coordinations",
    "unsettled_branch_ids",
    "failed_branch_ids",
    "inapplicable_branch_ids",
    "interrupted_branch_ids",
    "canceled_branch_ids",
    "join_ready",
    "resolved_join_plan",
    "join_state",
    "join_settlement",
    "unsettled_work_ids",
    "branch_set_succeeded",
    "coordination_settlement",
    "retirement_requested",
    "coordination_complete_for_retirement"
  ],
  "title": "BranchSetEvaluation",
  "type": "object"
}
```
