# schemas: BranchWorkBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchworkbinding:dcbc29d4f7 -->

Stable parent/branch lineage for one ordinary child work identity.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: BranchWorkBinding
- `description`: Stable parent/branch lineage for one ordinary child work identity.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| `decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `kind` | no | type="string"; const="branch" |  |
| `parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchWorkBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0c3297729fbc53bc25d0dcb1bf587a91b7991848f5584c8248180f28b506ecd9 -->

```json
{
  "additionalProperties": false,
  "description": "Stable parent/branch lineage for one ordinary child work identity.",
  "properties": {
    "artifact_selection_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Artifact Selection Sha256",
      "type": "string"
    },
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "decision_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Decision Sha256",
      "type": "string"
    },
    "kind": {
      "const": "branch",
      "default": "branch",
      "title": "Kind",
      "type": "string"
    },
    "parent_work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Parent Work Id",
      "type": "string"
    }
  },
  "required": [
    "parent_work_id",
    "branch_id",
    "decision_sha256",
    "artifact_selection_sha256"
  ],
  "title": "BranchWorkBinding",
  "type": "object"
}
```
