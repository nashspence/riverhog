# schemas: BranchSetEvaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-branchsetevaluation:6fcbb2e4b9 -->

Entirely derived view over a plan and ordinary child/join results.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-dfc1e70c60"></a>

- <a id="s-b749aa723f"></a>`type`: `"object"`
- <a id="s-ab9d9c50b9"></a>`additionalProperties`: `false`
- <a id="s-8108b93382"></a>`description`: `"Entirely derived view over a plan and ordinary child/join results."`
- <a id="s-b5d77b0902"></a>`required`: `["branch_set_sha256","succeeded_branches","succeeded_effects","succeeded_coordinations","unsettled_branch_ids","failed_branch_ids","inapplicable_branch_ids","interrupted_branch_ids","canceled_branch_ids","join_ready","resolved_join_plan","join_state","join_settlement","unsettled_work_ids","branch_set_succeeded","coordination_settlement","source_collection_retirement_requested","coordination_complete_for_retirement"]`
- <a id="s-759586f947"></a>`title`: `"BranchSetEvaluation"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-94c259f034"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Branch Set Sha256" |  |
| <a id="s-0b8012e8f1"></a>`branch_set_succeeded` | yes | type="boolean"; title="Branch Set Succeeded" |  |
| <a id="s-28bd0e47e6"></a>`canceled_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Canceled Branch Ids" |  |
| <a id="s-63dff433e7"></a>`coordination_complete_for_retirement` | yes | type="boolean"; title="Coordination Complete For Retirement" |  |
| <a id="s-f616c02dd4"></a>`coordination_settlement` | yes | anyOf=[([CoordinationSettlement](schemas-coordinationsettlement.md)); (type="null")] |  |
| <a id="s-7a8d5bbcfe"></a>`failed_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Failed Branch Ids" |  |
| <a id="s-5784e98faa"></a>`inapplicable_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Inapplicable Branch Ids" |  |
| <a id="s-5e39f03291"></a>`interrupted_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Interrupted Branch Ids" |  |
| <a id="s-88cf89f944"></a>`join_ready` | yes | type="boolean"; title="Join Ready" |  |
| <a id="s-451ecf2f60"></a>`join_settlement` | yes | anyOf=[([JoinSettlement](schemas-joinsettlement.md)); (type="null")] |  |
| <a id="s-61ce399152"></a>`join_state` | yes | type="string"; enum=["not-declared","waiting","ready","succeeded","failed","inapplicable","interrupted","canceled"]; title="Join State" |  |
| <a id="s-580c35fecd"></a>`resolved_join_plan` | yes | anyOf=[([JoinPlan](schemas-joinplan.md)); (type="null")] |  |
| <a id="s-c1089e4e09"></a>`source_collection_retirement_requested` | yes | type="boolean"; title="Source Collection Retirement Requested" |  |
| <a id="s-532021a47d"></a>`succeeded_branches` | yes | type="array"; items=([BranchSettlement](schemas-branchsettlement.md)); title="Succeeded Branches" |  |
| <a id="s-454b7a1bee"></a>`succeeded_coordinations` | yes | type="array"; items=([CoordinationSettlement](schemas-coordinationsettlement.md)); title="Succeeded Coordinations" |  |
| <a id="s-e849acf146"></a>`succeeded_effects` | yes | type="array"; items=([BranchEffectSettlement](schemas-brancheffectsettlement.md)); title="Succeeded Effects" |  |
| <a id="s-4cf1935a3b"></a>`unsettled_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Unsettled Branch Ids" |  |
| <a id="s-c9d8f273f5"></a>`unsettled_work_ids` | yes | type="array"; items=(type="string"; pattern="^[0-9a-f]{64}$"); title="Unsettled Work Ids" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field canceled_branch_ids](#s-28bd0e47e6) | `cardinality · items · operational_policy` | shared above |
| [field failed_branch_ids](#s-7a8d5bbcfe) | `cardinality · items · operational_policy` | shared above |
| [field inapplicable_branch_ids](#s-5784e98faa) | `cardinality · items · operational_policy` | shared above |
| [field interrupted_branch_ids](#s-5e39f03291) | `cardinality · items · operational_policy` | shared above |
| [field succeeded_branches](#s-532021a47d) | `cardinality · items · operational_policy` | shared above |
| [field succeeded_coordinations](#s-454b7a1bee) | `cardinality · items · operational_policy` | shared above |
| [field succeeded_effects](#s-e849acf146) | `cardinality · items · operational_policy` | shared above |
| [field unsettled_branch_ids](#s-4cf1935a3b) | `cardinality · items · operational_policy` | shared above |
| [field unsettled_work_ids](#s-c9d8f273f5) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_set_sha256](#s-94c259f034) | `length · characters · fixed` | shared above |
| <a id="s-d47cb1590a"></a>[field unsettled_work_ids · items](#s-c9d8f273f5) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [BranchEffectSettlement](schemas-brancheffectsettlement.md)
- [BranchSettlement](schemas-branchsettlement.md)
- [CoordinationSettlement](schemas-coordinationsettlement.md)
- [JoinPlan](schemas-joinplan.md)
- [JoinSettlement](schemas-joinsettlement.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-731bba465f"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-3ee0a547bd"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-25daed2150"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetEvaluation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8237b579ec1a4931023cc21bc104febfb25492bc35af2c2484e61a5458c9de70 -->

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
    "source_collection_retirement_requested": {
      "title": "Source Collection Retirement Requested",
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
    "source_collection_retirement_requested",
    "coordination_complete_for_retirement"
  ],
  "title": "BranchSetEvaluation",
  "type": "object"
}
```

</details>
