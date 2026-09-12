# schemas: PreviewAcceptanceView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-previewacceptanceview:1921dbce59 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/PreviewAcceptanceView`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: PreviewTargetExpectationView](schemas-previewtargetexpectationview.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: PreviewAcceptanceView
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `branch_set_sha256` | yes | string |  |
| `preview_sha256` | yes | string |  |
| `target_plans` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e903a5a8059f4c069db940e9fbb02a433f66d3d048c897077e9e35dcac7e90d7 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
      "type": "string"
    },
    "preview_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Preview Sha256",
      "type": "string"
    },
    "target_plans": {
      "items": {
        "$ref": "#/components/schemas/PreviewTargetExpectationView"
      },
      "title": "Target Plans",
      "type": "array"
    }
  },
  "required": [
    "preview_sha256",
    "branch_set_sha256",
    "target_plans"
  ],
  "title": "PreviewAcceptanceView",
  "type": "object"
}
```
