# schemas: BranchTargetPreview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-branchtargetpreview:16c922c7e5 -->

Target-owned preflight evidence for one exact previewed branch.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8bf7127c83"></a>

- <a id="s-6aea1f924b"></a>`type`: `"object"`
- <a id="s-b749f5aef1"></a>`additionalProperties`: `false`
- <a id="s-6b5674eb99"></a>`description`: `"Target-owned preflight evidence for one exact previewed branch."`
- <a id="s-2f76ae0dba"></a>`required`: `["branch_id","work_id","workflow_plan_sha256","target_plan"]`
- <a id="s-8929193df9"></a>`title`: `"BranchTargetPreview"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6503d656f4"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-ca3e436442"></a>`target_plan` | yes | [TargetPlanBinding](schemas-targetplanbinding.md) |  |
| <a id="s-7f97f4bd37"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |
| <a id="s-345304ff24"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workflow Plan Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field work_id](#s-7f97f4bd37) | `length · characters · fixed` | shared above |
| [field workflow_plan_sha256](#s-345304ff24) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [TargetPlanBinding](schemas-targetplanbinding.md)

## Governing policies

- <a id="pa-a70cd6f980"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-af18f531ef"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchTargetPreview`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 686f82e61ce31a8aa926d3ad139cbed42da92032d71f25ec0f781c9d7c6cc206 -->

```json
{
  "additionalProperties": false,
  "description": "Target-owned preflight evidence for one exact previewed branch.",
  "properties": {
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "target_plan": {
      "$ref": "#/components/schemas/TargetPlanBinding"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    },
    "workflow_plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Workflow Plan Sha256",
      "type": "string"
    }
  },
  "required": [
    "branch_id",
    "work_id",
    "workflow_plan_sha256",
    "target_plan"
  ],
  "title": "BranchTargetPreview",
  "type": "object"
}
```

</details>
