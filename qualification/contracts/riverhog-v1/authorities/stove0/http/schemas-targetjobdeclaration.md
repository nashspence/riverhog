# schemas: TargetJobDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetjobdeclaration:1c2286c704 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: TargetJobDeclaration
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `claim_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| `controller_evidence` | yes | #/components/schemas/ControllerEvidence |  |
| `fence` | yes | type="integer"; minimum=1 |  |
| `job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `plan` | yes | oneOf=#/components/schemas/TransformPlan \| #/components/schemas/EffectPlan; additional keys=`discriminator` |  |
| `workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"] |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ControllerEvidence](schemas-controllerevidence.md)
- [schemas: EffectPlan](schemas-effectplan.md)
- [schemas: TransformPlan](schemas-transformplan.md)

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

- `/external_contract/http_openapi/stove0/components/schemas/TargetJobDeclaration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 931894652141910434181fada4867f2288433e65db2fa98abec088e78533ef4a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "claim_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Claim Id",
      "type": "string"
    },
    "controller_evidence": {
      "$ref": "#/components/schemas/ControllerEvidence"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "job_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Job Id",
      "type": "string"
    },
    "plan": {
      "discriminator": {
        "mapping": {
          "stove0-effect-target/v1": "#/components/schemas/EffectPlan",
          "stove0-transform-target/v1": "#/components/schemas/TransformPlan"
        },
        "propertyName": "protocol"
      },
      "oneOf": [
        {
          "$ref": "#/components/schemas/TransformPlan"
        },
        {
          "$ref": "#/components/schemas/EffectPlan"
        }
      ],
      "title": "Plan"
    },
    "workspace_assurance": {
      "enum": [
        "encrypted",
        "ephemeral"
      ],
      "title": "Workspace Assurance",
      "type": "string"
    }
  },
  "required": [
    "job_id",
    "claim_id",
    "fence",
    "controller_evidence",
    "plan",
    "workspace_assurance"
  ],
  "title": "TargetJobDeclaration",
  "type": "object"
}
```
