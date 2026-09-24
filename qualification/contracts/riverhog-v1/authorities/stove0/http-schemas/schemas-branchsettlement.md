# schemas: BranchSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-branchsettlement:dd12ad1a2e -->

Success-only, Riverhog-verified result of one branch workflow plan.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-408a56022c"></a>

- <a id="s-320b6230bf"></a>`type`: `"object"`
- <a id="s-49ac7a3c9e"></a>`additionalProperties`: `false`
- <a id="s-9b0d642fa2"></a>`description`: `"Success-only, Riverhog-verified result of one branch workflow plan."`
- <a id="s-670b7e2ea8"></a>`required`: `["branch_id","work_id","workflow_plan_sha256","derivation_sha256","producer_settlement_sha256","output_collection","output_selection","settlement_sha256"]`
- <a id="s-d7c8b30fe8"></a>`title`: `"BranchSettlement"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d14fc4aaa1"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-b122415c3e"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Derivation Sha256" |  |
| <a id="s-8cc42659eb"></a>`format` | no | type="string"; const="stove0-branch-settlement/v1"; default="stove0-branch-settlement/v1"; title="Format" |  |
| <a id="s-9a2f0035d4"></a>`output_collection` | yes | [CollectionRootIdentityRef](schemas-collectionrootidentityref.md) |  |
| <a id="s-3523e63710"></a>`output_selection` | yes | [ArtifactSelectionRef](schemas-artifactselectionref.md) |  |
| <a id="s-3aa87a4f91"></a>`producer_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Producer Settlement Sha256" |  |
| <a id="s-786e03f388"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Settlement Sha256" |  |
| <a id="s-047c17c06f"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |
| <a id="s-0e747befdd"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workflow Plan Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field derivation_sha256](#s-b122415c3e) | `length · characters · fixed` | shared above |
| [field producer_settlement_sha256](#s-3aa87a4f91) | `length · characters · fixed` | shared above |
| [field settlement_sha256](#s-786e03f388) | `length · characters · fixed` | shared above |
| [field work_id](#s-047c17c06f) | `length · characters · fixed` | shared above |
| [field workflow_plan_sha256](#s-0e747befdd) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArtifactSelectionRef](schemas-artifactselectionref.md)
- [CollectionRootIdentityRef](schemas-collectionrootidentityref.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-63113127fd"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-b7e81f3762"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSettlement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e5db07b4773adf51d626c7c7b55be697ea8587f7bca61a95e87b9a108328c416 -->

```json
{
  "additionalProperties": false,
  "description": "Success-only, Riverhog-verified result of one branch workflow plan.",
  "properties": {
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "derivation_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Derivation Sha256",
      "type": "string"
    },
    "format": {
      "const": "stove0-branch-settlement/v1",
      "default": "stove0-branch-settlement/v1",
      "title": "Format",
      "type": "string"
    },
    "output_collection": {
      "$ref": "#/components/schemas/CollectionRootIdentityRef"
    },
    "output_selection": {
      "$ref": "#/components/schemas/ArtifactSelectionRef"
    },
    "producer_settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Producer Settlement Sha256",
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
    "derivation_sha256",
    "producer_settlement_sha256",
    "output_collection",
    "output_selection",
    "settlement_sha256"
  ],
  "title": "BranchSettlement",
  "type": "object"
}
```

</details>
