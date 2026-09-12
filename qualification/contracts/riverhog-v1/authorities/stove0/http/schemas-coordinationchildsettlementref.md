# schemas: CoordinationChildSettlementRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-coordinationchildsettlementref:4346726cf5 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CoordinationChildSettlementRef`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: CoordinationChildSettlementRef
- `description`: Exact direct-child success included in a coordination settlement.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `branch_id` | yes | string |  |
| `kind` | yes | string |  |
| `settlement_sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7bd31023cc09897af3321f85d7438ce508e1093e2abf51326d29362ab02bb869 -->

```json
{
  "additionalProperties": false,
  "description": "Exact direct-child success included in a coordination settlement.",
  "properties": {
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "kind": {
      "enum": [
        "collection",
        "external-effect",
        "coordination"
      ],
      "title": "Kind",
      "type": "string"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
    }
  },
  "required": [
    "branch_id",
    "kind",
    "settlement_sha256"
  ],
  "title": "CoordinationChildSettlementRef",
  "type": "object"
}
```
