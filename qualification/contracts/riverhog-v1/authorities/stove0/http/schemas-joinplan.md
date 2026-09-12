# schemas: JoinPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-joinplan:8ee030705d -->

Resolved ordinary join work over exact successful branch outputs.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-e8e5828f9d"></a>
- <a id="s-670a142fd6"></a>`title`: JoinPlan
- <a id="s-d819568938"></a>`description`: Resolved ordinary join work over exact successful branch outputs.
- <a id="s-80474eb93b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a5ba769baf"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1330e552ba"></a>`declaration` | yes | #/components/schemas/JoinDeclaration |  |
| <a id="s-66b0e81086"></a>`format` | no | type="string"; const="stove0-join-plan/v1" |  |
| <a id="s-d233798083"></a>`inputs` | yes | type="array"; minItems=2; items=(#/components/schemas/JoinInputPlan) |  |
| <a id="s-280eacdfc4"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-157328a94c"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bc2d25649d"></a>`work` | yes | #/components/schemas/WorkIdentity |  |
| <a id="s-2552103e52"></a>`workflow_plan` | yes | #/components/schemas/WorkflowPlan |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field inputs](#s-d233798083) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_set_sha256](#s-a5ba769baf) | `length · characters · fixed` | shared above |
| [field join_plan_sha256](#s-280eacdfc4) | `length · characters · fixed` | shared above |
| [field parent_work_id](#s-157328a94c) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JoinDeclaration](schemas-joindeclaration.md)
- [schemas: JoinInputPlan](schemas-joininputplan.md)
- [schemas: WorkIdentity](schemas-workidentity.md)
- [schemas: WorkflowPlan](schemas-workflowplan.md)

## Governing policies

- <a id="pa-de3170c291"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-ed475282c9"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-2cccb19be3"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
