# schemas: RecipeJoinMember

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-recipejoinmember:34e423aec6 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/RecipeJoinMember`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: RecipeJoinMember
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `branch_id` | yes | string |  |
| `output_roles` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f96f0cc79aff67369a99fa4bb8d4b8f29e89bdd258e990af58d3e937574ad43f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "output_roles": {
      "items": {
        "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
        "type": "string"
      },
      "minItems": 1,
      "title": "Output Roles",
      "type": "array"
    }
  },
  "required": [
    "branch_id",
    "output_roles"
  ],
  "title": "RecipeJoinMember",
  "type": "object"
}
```
