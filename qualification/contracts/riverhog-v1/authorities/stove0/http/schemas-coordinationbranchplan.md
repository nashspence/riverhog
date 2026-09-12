# schemas: CoordinationBranchPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-coordinationbranchplan:ff56b76dc5 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CoordinationBranchPlan`

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
- [schemas: WorkIdentity](schemas-workidentity.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: CoordinationBranchPlan
- `description`: One named required branch-bound child coordinator.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_selection` | yes | #/components/schemas/ArtifactSelectionRef |  |
| `branch_id` | yes | string |  |
| `branch_set_sha256` | yes | string |  |
| `kind` | no | string |  |
| `work` | yes | #/components/schemas/WorkIdentity |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ed4d6bb61339aec91bfadc7d21a769150b8f2dc2f983efb7e3c13f7f8c154313 -->

```json
{
  "additionalProperties": false,
  "description": "One named required branch-bound child coordinator.",
  "properties": {
    "artifact_selection": {
      "$ref": "#/components/schemas/ArtifactSelectionRef"
    },
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
      "type": "string"
    },
    "kind": {
      "const": "coordination",
      "default": "coordination",
      "title": "Kind",
      "type": "string"
    },
    "work": {
      "$ref": "#/components/schemas/WorkIdentity"
    }
  },
  "required": [
    "branch_id",
    "artifact_selection",
    "work",
    "branch_set_sha256"
  ],
  "title": "CoordinationBranchPlan",
  "type": "object"
}
```
