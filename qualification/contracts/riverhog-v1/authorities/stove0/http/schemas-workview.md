# schemas: WorkView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workview:754d420c65 -->

Operator projection of mutable work; never an execution identity.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 6 |

## External contract

<a id="s-69e6f54265"></a>
- <a id="s-04d9fb929b"></a>`title`: WorkView
- <a id="s-073263515f"></a>`description`: Operator projection of mutable work; never an execution identity.
- <a id="s-c46c0a68b2"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c69239134c"></a>`abandon_outcome` | no | anyOf=type="string"; enum=["inapplicable","failed","canceled"] \| type="null" |  |
| <a id="s-d9d46d49c0"></a>`branch_set_plan` | no | anyOf=#/components/schemas/BranchSetPlan \| type="null" |  |
| <a id="s-08fea03bf8"></a>`claim` | no | anyOf=#/components/schemas/WorkClaimView \| type="null" |  |
| <a id="s-42584f4aa1"></a>`controller_evidence` | no | anyOf=#/components/schemas/ControllerEvidence \| type="null" |  |
| <a id="s-99a30a713a"></a>`coordination_cancel_requested` | no | type="boolean" |  |
| <a id="s-f050d1c364"></a>`coordination_settlement` | no | anyOf=#/components/schemas/CoordinationSettlement \| type="null" |  |
| <a id="s-3e04aba910"></a>`expected_target_plan_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-47052796f8"></a>`failure` | no | anyOf=#/components/schemas/WorkFailureView \| type="null" |  |
| <a id="s-4c1ffdf0d3"></a>`format` | no | type="string"; const="stove0-work-view/v1" |  |
| <a id="s-ad37985d90"></a>`inapplicable` | no | anyOf=#/components/schemas/WorkInapplicableView \| type="null" |  |
| <a id="s-9ab84bc2ad"></a>`join_plan` | no | anyOf=#/components/schemas/JoinPlan \| type="null" |  |
| <a id="s-0d80f7addf"></a>`observation_requests` | no | type="array"; items=(#/components/schemas/ObservationRequest) |  |
| <a id="s-33173849b4"></a>`observation_results` | no | type="array"; items=(#/components/schemas/ObservationResult) |  |
| <a id="s-7cd6caa0b2"></a>`output` | no | anyOf=#/components/schemas/OutputCollectionRef \| type="null" |  |
| <a id="s-426ffe9458"></a>`phase` | yes | type="string"; enum=["eligible","claimed","observing","planning","target_preflight","queued","executing","output_finalizing","verifying","settled","retirement_pending","coordinating","abandon_pending","complete","inapplicable","failed","canceled"] |  |
| <a id="s-46d5cbd753"></a>`preview_acceptance` | no | anyOf=#/components/schemas/PreviewAcceptanceView \| type="null" |  |
| <a id="s-6a0a7b056c"></a>`retirement_remaining` | no | type="array"; items=(type="integer") |  |
| <a id="s-59b5cf38e7"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-089cf18873"></a>`target_plan` | no | anyOf=oneOf=#/components/schemas/TransformPlan \| #/components/schemas/EffectPlan; additional keys=`discriminator` \| type="null" |  |
| <a id="s-3238506934"></a>`target_request` | no | anyOf=#/components/schemas/AcceptedTargetJob \| type="null" |  |
| <a id="s-47c9f56e77"></a>`target_settlement` | no | anyOf=#/components/schemas/TargetSettlementAuthority \| type="null" |  |
| <a id="s-191b9a3de7"></a>`target_status` | no | anyOf=#/components/schemas/TargetJobStatus \| type="null" |  |
| <a id="s-7fdbb2aaa6"></a>`work` | yes | #/components/schemas/WorkIdentity |  |
| <a id="s-6b0ef08835"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b23ed55dc5"></a>`workflow_plan` | no | anyOf=#/components/schemas/WorkflowPlan \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field observation_requests](#s-0d80f7addf) | `cardinality · items · operational_policy` | shared above |
| [field observation_results](#s-33173849b4) | `cardinality · items · operational_policy` | shared above |
| <a id="s-b25631da5d"></a>[field retirement_remaining · items](#s-6a0a7b056c) | `value · schema-value · operational_policy` | shared above |
| [field retirement_remaining](#s-6a0a7b056c) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-c521ae1f95"></a>[field expected_target_plan_sha256 · string value](#s-3e04aba910) | `length · characters · fixed` | shared above |
| [field work_id](#s-6b0ef08835) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

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

## Governing policies

- <a id="pa-4f51da8a3b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-d977b6900f"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-6960819304"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkView`

### Exact owned JSON

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
