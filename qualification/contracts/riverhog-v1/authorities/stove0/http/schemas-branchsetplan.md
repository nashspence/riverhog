# schemas: BranchSetPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchsetplan:19bbb0cc9b -->

One immutable set of required branches and one optional exact join.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-df3b3ca9c2"></a>
- <a id="s-9669fa3e5c"></a>`title`: BranchSetPlan
- <a id="s-d18dd59d55"></a>`description`: One immutable set of required branches and one optional exact join.
- <a id="s-6d1665c52c"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fb4dbc4b31"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dfc8667881"></a>`branches` | yes | type="array"; minItems=1; items=(oneOf=#/components/schemas/BranchPlan \| #/components/schemas/CoordinationBranchPlan; additional keys=`discriminator`) |  |
| <a id="s-32fa6f6e6c"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-80f9b8d777"></a>`evidence_sha256s` | no | type="array"; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-3b5284256e"></a>`format` | no | type="string"; const="stove0-branch-set/v1" |  |
| <a id="s-debaf9ead5"></a>`join` | no | anyOf=#/components/schemas/JoinDeclaration \| type="null" |  |
| <a id="s-254366e7ed"></a>`parent_work` | yes | #/components/schemas/WorkIdentity |  |
| <a id="s-347ef767d1"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0 |  |
| <a id="s-6ba67858d5"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branches](#s-dfc8667881) | `cardinality · items · operational_policy` | shared above |
| [field evidence_sha256s](#s-80f9b8d777) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_set_sha256](#s-fb4dbc4b31) | `length · characters · fixed` | shared above |
| [field decision_sha256](#s-32fa6f6e6c) | `length · characters · fixed` | shared above |
| <a id="s-6ec4f4098a"></a>[field evidence_sha256s · items](#s-80f9b8d777) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BranchPlan](schemas-branchplan.md)
- [schemas: CoordinationBranchPlan](schemas-coordinationbranchplan.md)
- [schemas: JoinDeclaration](schemas-joindeclaration.md)
- [schemas: WorkIdentity](schemas-workidentity.md)

## Governing policies

- <a id="pa-0da48dc7dc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-2baaa09962"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-84dd4b9a45"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetPlan`

### Exact owned JSON

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
