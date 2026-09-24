# schemas: BranchEffectSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-brancheffectsettlement:6a88b5333b -->

Success-only receipt identity for one required external-effect branch.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8c1e5f0939"></a>

- <a id="s-11150d17c0"></a>`type`: `"object"`
- <a id="s-4dbd09cc6c"></a>`additionalProperties`: `false`
- <a id="s-0814e85c2a"></a>`description`: `"Success-only receipt identity for one required external-effect branch."`
- <a id="s-ab761040b7"></a>`required`: `["branch_id","work_id","workflow_plan_sha256","effect_receipt_sha256","settlement_sha256"]`
- <a id="s-2a67007b4a"></a>`title`: `"BranchEffectSettlement"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-864c5c9db0"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-3316e047b7"></a>`effect_receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Effect Receipt Sha256" |  |
| <a id="s-a8662c1dfd"></a>`format` | no | type="string"; const="stove0-branch-effect-settlement/v1"; default="stove0-branch-effect-settlement/v1"; title="Format" |  |
| <a id="s-c9cc6c58a2"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Settlement Sha256" |  |
| <a id="s-c1a292eac6"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |
| <a id="s-7b654d920a"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workflow Plan Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effect_receipt_sha256](#s-3316e047b7) | `length · characters · fixed` | shared above |
| [field settlement_sha256](#s-c9cc6c58a2) | `length · characters · fixed` | shared above |
| [field work_id](#s-c1a292eac6) | `length · characters · fixed` | shared above |
| [field workflow_plan_sha256](#s-7b654d920a) | `length · characters · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-41c124631c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-b62f3c55f5"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchEffectSettlement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d079076de524eab27cd12f57634171620d2cd76204c4d4349a9231ece5830d8 -->

```json
{
  "additionalProperties": false,
  "description": "Success-only receipt identity for one required external-effect branch.",
  "properties": {
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "effect_receipt_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Effect Receipt Sha256",
      "type": "string"
    },
    "format": {
      "const": "stove0-branch-effect-settlement/v1",
      "default": "stove0-branch-effect-settlement/v1",
      "title": "Format",
      "type": "string"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
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
    "effect_receipt_sha256",
    "settlement_sha256"
  ],
  "title": "BranchEffectSettlement",
  "type": "object"
}
```

</details>
