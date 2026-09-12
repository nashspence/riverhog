# schemas: WorkflowPreview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workflowpreview:71eacf8046 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

- `title`: WorkflowPreview
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `branch_set_plan` | no | anyOf=#/components/schemas/BranchSetPlan \| type="null" |  |
| `branch_sets` | no | type="array"; items=(#/components/schemas/BranchSetPlan) |  |
| `format` | no | type="string"; const="stove0-workflow-preview/v1" |  |
| `observations` | no | type="array"; items=(#/components/schemas/ObservationEvidence) |  |
| `outcome` | no | anyOf=#/components/schemas/PreviewOutcome \| type="null" |  |
| `preview_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `selections` | no | type="array"; items=(#/components/schemas/ArtifactSelection) |  |
| `state` | yes | type="string"; enum=["ready","inapplicable","failed","canceled"] |  |
| `target_plans` | no | type="array"; items=(#/components/schemas/BranchTargetPreview) |  |
| `warnings` | no | type="array"; items=(type="string") |  |
| `work` | yes | #/components/schemas/WorkIdentity |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSelection](schemas-artifactselection.md)
- [schemas: BranchSetPlan](schemas-branchsetplan.md)
- [schemas: BranchTargetPreview](schemas-branchtargetpreview.md)
- [schemas: ObservationEvidence](schemas-observationevidence.md)
- [schemas: PreviewOutcome](schemas-previewoutcome.md)
- [schemas: WorkIdentity](schemas-workidentity.md)

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

- `/external_contract/http_openapi/stove0/components/schemas/WorkflowPreview`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0891d6903a60a9ac3c1ff0faaef93b801d2931190371903616f95a2f55110d2b -->

```json
{
  "additionalProperties": false,
  "properties": {
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
    "branch_sets": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/BranchSetPlan"
      },
      "title": "Branch Sets",
      "type": "array"
    },
    "format": {
      "const": "stove0-workflow-preview/v1",
      "default": "stove0-workflow-preview/v1",
      "title": "Format",
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
    "outcome": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/PreviewOutcome"
        },
        {
          "type": "null"
        }
      ]
    },
    "preview_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Preview Id",
      "type": "string"
    },
    "preview_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Preview Sha256",
      "type": "string"
    },
    "selections": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/ArtifactSelection"
      },
      "title": "Selections",
      "type": "array"
    },
    "state": {
      "enum": [
        "ready",
        "inapplicable",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    },
    "target_plans": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/BranchTargetPreview"
      },
      "title": "Target Plans",
      "type": "array"
    },
    "warnings": {
      "default": [],
      "items": {
        "type": "string"
      },
      "title": "Warnings",
      "type": "array"
    },
    "work": {
      "$ref": "#/components/schemas/WorkIdentity"
    }
  },
  "required": [
    "preview_id",
    "state",
    "work",
    "preview_sha256"
  ],
  "title": "WorkflowPreview",
  "type": "object"
}
```
