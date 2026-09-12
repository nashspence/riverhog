# schemas: JoinWorkBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-joinworkbinding:7eb3505b26 -->

Stable branch-set lineage for one ordinary join work identity.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: JoinWorkBinding
- `description`: Stable branch-set lineage for one ordinary join work identity.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `kind` | no | type="string"; const="join" |  |
| `members` | yes | type="array"; minItems=2; items=(#/components/schemas/JoinWorkMemberBinding) |  |
| `parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JoinWorkMemberBinding](schemas-joinworkmemberbinding.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinWorkBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f2994411487c2f5a213ce07a72a8092d94a0e6a56dcccb7110296f6f222b472 -->

```json
{
  "additionalProperties": false,
  "description": "Stable branch-set lineage for one ordinary join work identity.",
  "properties": {
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
      "type": "string"
    },
    "kind": {
      "const": "join",
      "default": "join",
      "title": "Kind",
      "type": "string"
    },
    "members": {
      "items": {
        "$ref": "#/components/schemas/JoinWorkMemberBinding"
      },
      "minItems": 2,
      "title": "Members",
      "type": "array"
    },
    "parent_work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Parent Work Id",
      "type": "string"
    }
  },
  "required": [
    "parent_work_id",
    "branch_set_sha256",
    "members"
  ],
  "title": "JoinWorkBinding",
  "type": "object"
}
```
