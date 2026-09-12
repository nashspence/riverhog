# schemas: WorkflowPreview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workflowpreview:71eacf8046 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

<a id="s-f0f07e63bd"></a>
- <a id="s-aa6d1cb984"></a>`title`: WorkflowPreview
- <a id="s-482360a6a8"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-09d8a2f675"></a>`branch_set_plan` | no | anyOf=#/components/schemas/BranchSetPlan \| type="null" |  |
| <a id="s-af86a5605a"></a>`branch_sets` | no | type="array"; items=(#/components/schemas/BranchSetPlan) |  |
| <a id="s-3803d68986"></a>`format` | no | type="string"; const="stove0-workflow-preview/v1" |  |
| <a id="s-55d0cba4ef"></a>`observations` | no | type="array"; items=(#/components/schemas/ObservationEvidence) |  |
| <a id="s-e4744c1f84"></a>`outcome` | no | anyOf=#/components/schemas/PreviewOutcome \| type="null" |  |
| <a id="s-715057aea7"></a>`preview_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3c3b2303ab"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-af19f564b7"></a>`selections` | no | type="array"; items=(#/components/schemas/ArtifactSelection) |  |
| <a id="s-842de9abc9"></a>`state` | yes | type="string"; enum=["ready","inapplicable","failed","canceled"] |  |
| <a id="s-a984bdec16"></a>`target_plans` | no | type="array"; items=(#/components/schemas/BranchTargetPreview) |  |
| <a id="s-ed22ffece9"></a>`warnings` | no | type="array"; items=(type="string") |  |
| <a id="s-807a7c0d8b"></a>`work` | yes | #/components/schemas/WorkIdentity |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_sets](#s-af86a5605a) | `cardinality · items · operational_policy` | shared above |
| [field observations](#s-55d0cba4ef) | `cardinality · items · operational_policy` | shared above |
| [field selections](#s-af19f564b7) | `cardinality · items · operational_policy` | shared above |
| [field target_plans](#s-a984bdec16) | `cardinality · items · operational_policy` | shared above |
| [field warnings](#s-ed22ffece9) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field preview_id](#s-715057aea7) | `length · characters · fixed` | shared above |
| [field preview_sha256](#s-3c3b2303ab) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSelection](schemas-artifactselection.md)
- [schemas: BranchSetPlan](schemas-branchsetplan.md)
- [schemas: BranchTargetPreview](schemas-branchtargetpreview.md)
- [schemas: ObservationEvidence](schemas-observationevidence.md)
- [schemas: PreviewOutcome](schemas-previewoutcome.md)
- [schemas: WorkIdentity](schemas-workidentity.md)

## Governing policies

- <a id="pa-83bba4182e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-f1136339b3"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-7bfff570b9"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
