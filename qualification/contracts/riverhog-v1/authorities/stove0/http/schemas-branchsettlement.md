# schemas: BranchSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchsettlement:9a4371a5e9 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSettlement`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactSelectionRef](schemas-artifactselectionref.md)
- [schemas: CollectionRootRef](schemas-collectionrootref.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: BranchSettlement
- `description`: Success-only, Riverhog-verified result of one branch workflow plan.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `branch_id` | yes | string |  |
| `derivation_sha256` | yes | string |  |
| `format` | no | string |  |
| `output_collection` | yes | #/components/schemas/CollectionRootRef |  |
| `output_selection` | yes | #/components/schemas/ArtifactSelectionRef |  |
| `producer_settlement_sha256` | yes | string |  |
| `settlement_sha256` | yes | string |  |
| `work_id` | yes | string |  |
| `workflow_plan_sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff3841fca031acff04876a69b8405ed3e2b279985c48de34ca24abbe802cbf08 -->

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
      "$ref": "#/components/schemas/CollectionRootRef"
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
