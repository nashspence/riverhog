# schemas: BranchSetPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchsetplan:19bbb0cc9b -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetPlan`

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

- [schemas: BranchPlan](schemas-branchplan.md)
- [schemas: CoordinationBranchPlan](schemas-coordinationbranchplan.md)
- [schemas: JoinDeclaration](schemas-joindeclaration.md)
- [schemas: WorkIdentity](schemas-workidentity.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: BranchSetPlan
- `description`: One immutable set of required branches and one optional exact join.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `branch_set_sha256` | yes | string |  |
| `branches` | yes | array |  |
| `decision_sha256` | yes | string |  |
| `evidence_sha256s` | no | array |  |
| `format` | no | string |  |
| `join` | no | object (1 fields) |  |
| `parent_work` | yes | #/components/schemas/WorkIdentity |  |
| `retirement_grace_seconds` | no | integer |  |
| `retirement_policy` | no | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8bdfd120eb1c50e402a9920f0a877174140baadc1d4b30dbc16afbb12d37734b -->

```json
{
  "additionalProperties": false,
  "description": "One immutable set of required branches and one optional exact join.",
  "properties": {
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
      "type": "string"
    },
    "branches": {
      "items": {
        "discriminator": {
          "mapping": {
            "coordination": "#/components/schemas/CoordinationBranchPlan",
            "leaf": "#/components/schemas/BranchPlan"
          },
          "propertyName": "kind"
        },
        "oneOf": [
          {
            "$ref": "#/components/schemas/BranchPlan"
          },
          {
            "$ref": "#/components/schemas/CoordinationBranchPlan"
          }
        ]
      },
      "minItems": 1,
      "title": "Branches",
      "type": "array"
    },
    "decision_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Decision Sha256",
      "type": "string"
    },
    "evidence_sha256s": {
      "default": [],
      "items": {
        "pattern": "^[0-9a-f]{64}$",
        "type": "string"
      },
      "title": "Evidence Sha256S",
      "type": "array"
    },
    "format": {
      "const": "stove0-branch-set/v1",
      "default": "stove0-branch-set/v1",
      "title": "Format",
      "type": "string"
    },
    "join": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/JoinDeclaration"
        },
        {
          "type": "null"
        }
      ]
    },
    "parent_work": {
      "$ref": "#/components/schemas/WorkIdentity"
    },
    "retirement_grace_seconds": {
      "default": 0,
      "minimum": 0,
      "title": "Retirement Grace Seconds",
      "type": "integer"
    },
    "retirement_policy": {
      "default": "retain",
      "enum": [
        "retain",
        "retire-after-verified-output"
      ],
      "title": "Retirement Policy",
      "type": "string"
    }
  },
  "required": [
    "parent_work",
    "decision_sha256",
    "branches",
    "branch_set_sha256"
  ],
  "title": "BranchSetPlan",
  "type": "object"
}
```
