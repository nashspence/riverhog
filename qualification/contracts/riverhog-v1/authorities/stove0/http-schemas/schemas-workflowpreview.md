# schemas: WorkflowPreview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workflowpreview:f3c6d1fd0a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f0f07e63bd"></a>

- <a id="s-482360a6a8"></a>`type`: `"object"`
- <a id="s-f77e1d277e"></a>`additionalProperties`: `false`
- <a id="s-542f4cf396"></a>`required`: `["preview_id","state","work","preview_sha256"]`
- <a id="s-aa6d1cb984"></a>`title`: `"WorkflowPreview"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-09d8a2f675"></a>`branch_set_plan` | no | anyOf=[([BranchSetPlan](schemas-branchsetplan.md)); (type="null")] |  |
| <a id="s-af86a5605a"></a>`branch_sets` | no | type="array"; default=[]; items=([BranchSetPlan](schemas-branchsetplan.md)); title="Branch Sets" |  |
| <a id="s-3803d68986"></a>`format` | no | type="string"; const="stove0-workflow-preview/v1"; default="stove0-workflow-preview/v1"; title="Format" |  |
| <a id="s-55d0cba4ef"></a>`observations` | no | type="array"; default=[]; items=([ObservationEvidence](schemas-observationevidence.md)); title="Observations" |  |
| <a id="s-e4744c1f84"></a>`outcome` | no | anyOf=[([PreviewOutcome](schemas-previewoutcome.md)); (type="null")] |  |
| <a id="s-715057aea7"></a>`preview_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Preview Id" |  |
| <a id="s-3c3b2303ab"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Preview Sha256" |  |
| <a id="s-af19f564b7"></a>`selections` | no | type="array"; default=[]; items=([ArtifactSelection](schemas-artifactselection.md)); title="Selections" |  |
| <a id="s-842de9abc9"></a>`state` | yes | type="string"; enum=["ready","inapplicable","failed","canceled"]; title="State" |  |
| <a id="s-a984bdec16"></a>`target_plans` | no | type="array"; default=[]; items=([BranchTargetPreview](schemas-branchtargetpreview.md)); title="Target Plans" |  |
| <a id="s-ed22ffece9"></a>`warnings` | no | type="array"; default=[]; items=(type="string"); title="Warnings" |  |
| <a id="s-807a7c0d8b"></a>`work` | yes | [WorkIdentity](schemas-workidentity.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_sets](#s-af86a5605a) | `cardinality · items · operational_policy` | shared above |
| [field observations](#s-55d0cba4ef) | `cardinality · items · operational_policy` | shared above |
| [field selections](#s-af19f564b7) | `cardinality · items · operational_policy` | shared above |
| [field target_plans](#s-a984bdec16) | `cardinality · items · operational_policy` | shared above |
| [field warnings](#s-ed22ffece9) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field preview_id](#s-715057aea7) | `length · characters · fixed` | shared above |
| [field preview_sha256](#s-3c3b2303ab) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArtifactSelection](schemas-artifactselection.md)
- [BranchSetPlan](schemas-branchsetplan.md)
- [BranchTargetPreview](schemas-branchtargetpreview.md)
- [ObservationEvidence](schemas-observationevidence.md)
- [PreviewOutcome](schemas-previewoutcome.md)
- [WorkIdentity](schemas-workidentity.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c954f21016"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-2c59a99bbc"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-102e3df726"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkflowPreview`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
