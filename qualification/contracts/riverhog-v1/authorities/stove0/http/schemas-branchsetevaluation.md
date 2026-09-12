# schemas: BranchSetEvaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchsetevaluation:7f019b5895 -->

Entirely derived view over a plan and ordinary child/join results.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 11 |

## External contract

<a id="s-dfc1e70c60d6"></a>
- <a id="s-759586f9473f"></a>`title`: BranchSetEvaluation
- <a id="s-8108b9338260"></a>`description`: Entirely derived view over a plan and ordinary child/join results.
- <a id="s-b749aa723f55"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-94c259f034ea"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0b8012e8f113"></a>`branch_set_succeeded` | yes | type="boolean" |  |
| <a id="s-28bd0e47e6fd"></a>`canceled_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-63dff433e7f7"></a>`coordination_complete_for_retirement` | yes | type="boolean" |  |
| <a id="s-f616c02dd4c0"></a>`coordination_settlement` | yes | anyOf=#/components/schemas/CoordinationSettlement \| type="null" |  |
| <a id="s-7a8d5bbcfea6"></a>`failed_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-5784e98faa2c"></a>`inapplicable_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-5e39f032913b"></a>`interrupted_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-88cf89f94445"></a>`join_ready` | yes | type="boolean" |  |
| <a id="s-451ecf2f608e"></a>`join_settlement` | yes | anyOf=#/components/schemas/JoinSettlement \| type="null" |  |
| <a id="s-61ce3991525b"></a>`join_state` | yes | type="string"; enum=["not-declared","waiting","ready","succeeded","failed","inapplicable","interrupted","canceled"] |  |
| <a id="s-580c35fecd21"></a>`resolved_join_plan` | yes | anyOf=#/components/schemas/JoinPlan \| type="null" |  |
| <a id="s-66b65c7b2127"></a>`retirement_requested` | yes | type="boolean" |  |
| <a id="s-532021a47d87"></a>`succeeded_branches` | yes | type="array"; items=(#/components/schemas/BranchSettlement) |  |
| <a id="s-454b7a1beedb"></a>`succeeded_coordinations` | yes | type="array"; items=(#/components/schemas/CoordinationSettlement) |  |
| <a id="s-e849acf14624"></a>`succeeded_effects` | yes | type="array"; items=(#/components/schemas/BranchEffectSettlement) |  |
| <a id="s-4cf1935a3b85"></a>`unsettled_branch_ids` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-c9d8f273f547"></a>`unsettled_work_ids` | yes | type="array"; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field canceled_branch_ids](#s-28bd0e47e6fd) | `cardinality · items · operational_policy` | shared above |
| [field failed_branch_ids](#s-7a8d5bbcfea6) | `cardinality · items · operational_policy` | shared above |
| [field inapplicable_branch_ids](#s-5784e98faa2c) | `cardinality · items · operational_policy` | shared above |
| [field interrupted_branch_ids](#s-5e39f032913b) | `cardinality · items · operational_policy` | shared above |
| [field succeeded_branches](#s-532021a47d87) | `cardinality · items · operational_policy` | shared above |
| [field succeeded_coordinations](#s-454b7a1beedb) | `cardinality · items · operational_policy` | shared above |
| [field succeeded_effects](#s-e849acf14624) | `cardinality · items · operational_policy` | shared above |
| [field unsettled_branch_ids](#s-4cf1935a3b85) | `cardinality · items · operational_policy` | shared above |
| [field unsettled_work_ids](#s-c9d8f273f547) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_set_sha256](#s-94c259f034ea) | `length · characters · fixed` | shared above |
| <a id="s-d47cb1590ab6"></a>field unsettled_work_ids · items | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BranchEffectSettlement](schemas-brancheffectsettlement.md)
- [schemas: BranchSettlement](schemas-branchsettlement.md)
- [schemas: CoordinationSettlement](schemas-coordinationsettlement.md)
- [schemas: JoinPlan](schemas-joinplan.md)
- [schemas: JoinSettlement](schemas-joinsettlement.md)

## Governing policies

- <a id="pa-6d8f805015d1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-00e9a9f9f852"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-25f761a1d950"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetEvaluation`

### Exact owned JSON

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
