# schemas: CoordinationSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-coordinationsettlement:27a57daf18 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CoordinationSettlement`

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

- [schemas: CoordinationChildSettlementRef](schemas-coordinationchildsettlementref.md)
- [schemas: CoordinationCollectionResult](schemas-coordinationcollectionresult.md)
- [schemas: WorkIdentity](schemas-workidentity.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: CoordinationSettlement
- `description`: Success-only exact completion of one root or branch-bound coordinator.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `branch_set_sha256` | yes | string |  |
| `children` | yes | array |  |
| `collection_result` | no | object (1 fields) |  |
| `contains_external_effects` | yes | boolean |  |
| `final_join_settlement_sha256` | no | object (2 fields) |  |
| `format` | no | string |  |
| `settlement_sha256` | yes | string |  |
| `work` | yes | #/components/schemas/WorkIdentity |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5fec61eb1a46933a79e3188e2861715cc83ed7e3bfe0c779e2fac77d41e13d59 -->

```json
{
  "additionalProperties": false,
  "description": "Success-only exact completion of one root or branch-bound coordinator.",
  "properties": {
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
      "type": "string"
    },
    "children": {
      "items": {
        "$ref": "#/components/schemas/CoordinationChildSettlementRef"
      },
      "title": "Children",
      "type": "array"
    },
    "collection_result": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CoordinationCollectionResult"
        },
        {
          "type": "null"
        }
      ]
    },
    "contains_external_effects": {
      "title": "Contains External Effects",
      "type": "boolean"
    },
    "final_join_settlement_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Final Join Settlement Sha256"
    },
    "format": {
      "const": "stove0-coordination-settlement/v1",
      "default": "stove0-coordination-settlement/v1",
      "title": "Format",
      "type": "string"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
    },
    "work": {
      "$ref": "#/components/schemas/WorkIdentity"
    }
  },
  "required": [
    "work",
    "branch_set_sha256",
    "children",
    "contains_external_effects",
    "settlement_sha256"
  ],
  "title": "CoordinationSettlement",
  "type": "object"
}
```
