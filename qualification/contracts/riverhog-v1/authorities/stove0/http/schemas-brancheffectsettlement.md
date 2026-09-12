# schemas: BranchEffectSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-brancheffectsettlement:4e4fc591ca -->

Success-only receipt identity for one required external-effect branch.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-8c1e5f093963"></a>
- <a id="s-2a67007b4a13"></a>`title`: BranchEffectSettlement
- <a id="s-0814e85c2af9"></a>`description`: Success-only receipt identity for one required external-effect branch.
- <a id="s-11150d17c0bb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-864c5c9db036"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3316e047b76e"></a>`effect_receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a8662c1dfd19"></a>`format` | no | type="string"; const="stove0-branch-effect-settlement/v1" |  |
| <a id="s-c9cc6c58a218"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c1a292eac60b"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-7b654d920ae2"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field effect_receipt_sha256](#s-3316e047b76e) | `length · characters · fixed` | shared above |
| [field settlement_sha256](#s-c9cc6c58a218) | `length · characters · fixed` | shared above |
| [field work_id](#s-c1a292eac60b) | `length · characters · fixed` | shared above |
| [field workflow_plan_sha256](#s-7b654d920ae2) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-5b03a34ccd39"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-fdf700a2959e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchEffectSettlement`

### Exact owned JSON

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
