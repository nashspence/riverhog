# schemas: WorkView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workview:754d420c65 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkView`

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

- [schemas: AcceptedTargetJob](schemas-acceptedtargetjob.md)
- [schemas: BranchSetPlan](schemas-branchsetplan.md)
- [schemas: ControllerEvidence](schemas-controllerevidence.md)
- [schemas: CoordinationSettlement](schemas-coordinationsettlement.md)
- [schemas: EffectPlan](schemas-effectplan.md)
- [schemas: JoinPlan](schemas-joinplan.md)
- [schemas: ObservationRequest](schemas-observationrequest.md)
- [schemas: ObservationResult](schemas-observationresult.md)
- [schemas: OutputCollectionRef](schemas-outputcollectionref.md)
- [schemas: PreviewAcceptanceView](schemas-previewacceptanceview.md)
- [schemas: TargetJobStatus](schemas-targetjobstatus.md)
- [schemas: TargetSettlementAuthority](schemas-targetsettlementauthority.md)
- [schemas: TransformPlan](schemas-transformplan.md)
- [schemas: WorkClaimView](schemas-workclaimview.md)
- [schemas: WorkFailureView](schemas-workfailureview.md)
- [schemas: WorkIdentity](schemas-workidentity.md)
- [schemas: WorkInapplicableView](schemas-workinapplicableview.md)
- [schemas: WorkflowPlan](schemas-workflowplan.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: WorkView
- `description`: Operator projection of mutable work; never an execution identity.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `abandon_outcome` | no | object (2 fields) |  |
| `branch_set_plan` | no | object (1 fields) |  |
| `claim` | no | object (1 fields) |  |
| `controller_evidence` | no | object (1 fields) |  |
| `coordination_cancel_requested` | no | boolean |  |
| `coordination_settlement` | no | object (1 fields) |  |
| `expected_target_plan_sha256` | no | object (2 fields) |  |
| `failure` | no | object (1 fields) |  |
| `format` | no | string |  |
| `inapplicable` | no | object (1 fields) |  |
| `join_plan` | no | object (1 fields) |  |
| `observation_requests` | no | array |  |
| `observation_results` | no | array |  |
| `output` | no | object (1 fields) |  |
| `phase` | yes | string |  |
| `preview_acceptance` | no | object (1 fields) |  |
| `retirement_remaining` | no | array |  |
| `revision` | yes | integer |  |
| `target_plan` | no | object (2 fields) |  |
| `target_request` | no | object (1 fields) |  |
| `target_settlement` | no | object (1 fields) |  |
| `target_status` | no | object (1 fields) |  |
| `work` | yes | #/components/schemas/WorkIdentity |  |
| `work_id` | yes | string |  |
| `workflow_plan` | no | object (1 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9841e3ec7c3884ed77d067b18336f060b4cf04a352ec1a59eca808ff340ecd1f -->

```json
{
  "additionalProperties": false,
  "description": "Operator projection of mutable work; never an execution identity.",
  "properties": {
    "abandon_outcome": {
      "anyOf": [
        {
          "enum": [
            "inapplicable",
            "failed",
            "canceled"
          ],
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Abandon Outcome"
    },
    "branch_set_plan": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/BranchSetPlan"
        },
        {
          "type": "null"
        }
      ]
    },
    "claim": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/WorkClaimView"
        },
        {
          "type": "null"
        }
      ]
    },
    "controller_evidence": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ControllerEvidence"
        },
        {
          "type": "null"
        }
      ]
    },
    "coordination_cancel_requested": {
      "default": false,
      "title": "Coordination Cancel Requested",
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
    "expected_target_plan_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Expected Target Plan Sha256"
    },
    "failure": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/WorkFailureView"
        },
        {
          "type": "null"
        }
      ]
    },
    "format": {
      "const": "stove0-work-view/v1",
      "default": "stove0-work-view/v1",
      "title": "Format",
      "type": "string"
    },
    "inapplicable": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/WorkInapplicableView"
        },
        {
          "type": "null"
        }
      ]
    },
    "join_plan": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/JoinPlan"
        },
        {
          "type": "null"
        }
      ]
    },
    "observation_requests": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/ObservationRequest"
      },
      "title": "Observation Requests",
      "type": "array"
    },
    "observation_results": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/ObservationResult"
      },
      "title": "Observation Results",
      "type": "array"
    },
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
    "phase": {
      "enum": [
        "eligible",
        "claimed",
        "observing",
        "planning",
        "target_preflight",
        "queued",
        "executing",
        "output_finalizing",
        "verifying",
        "settled",
        "retirement_pending",
        "coordinating",
        "abandon_pending",
        "complete",
        "inapplicable",
        "failed",
        "canceled"
      ],
      "title": "Phase",
      "type": "string"
    },
    "preview_acceptance": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/PreviewAcceptanceView"
        },
        {
          "type": "null"
        }
      ]
    },
    "retirement_remaining": {
      "default": [],
      "items": {
        "type": "integer"
      },
      "title": "Retirement Remaining",
      "type": "array"
    },
    "revision": {
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    },
    "target_plan": {
      "anyOf": [
        {
          "discriminator": {
            "mapping": {
              "stove0-effect-target/v1": "#/components/schemas/EffectPlan",
              "stove0-transform-target/v1": "#/components/schemas/TransformPlan"
            },
            "propertyName": "protocol"
          },
          "oneOf": [
            {
              "$ref": "#/components/schemas/TransformPlan"
            },
            {
              "$ref": "#/components/schemas/EffectPlan"
            }
          ]
        },
        {
          "type": "null"
        }
      ],
      "title": "Target Plan"
    },
    "target_request": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/AcceptedTargetJob"
        },
        {
          "type": "null"
        }
      ]
    },
    "target_settlement": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/TargetSettlementAuthority"
        },
        {
          "type": "null"
        }
      ]
    },
    "target_status": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/TargetJobStatus"
        },
        {
          "type": "null"
        }
      ]
    },
    "work": {
      "$ref": "#/components/schemas/WorkIdentity"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    },
    "workflow_plan": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/WorkflowPlan"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "work_id",
    "work",
    "phase",
    "revision"
  ],
  "title": "WorkView",
  "type": "object"
}
```
