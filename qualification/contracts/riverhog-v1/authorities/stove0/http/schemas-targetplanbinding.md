# schemas: TargetPlanBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetplanbinding:97956d1dde -->

Opaque binding to a target-owned preflight plan.

The target protocol owns the plan schema and canonicalization algorithm. stove0
retains the complete validated plan document and its target-issued digest, but
deliberately does not reinterpret or re-hash the plan with stove0's canonical
JSON rules. This prevents two authorities from disagreeing about target plan
identity while preserving the full document in the execution envelope.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

- `title`: TargetPlanBinding
- `description`: Opaque binding to a target-owned preflight plan.  The target protocol owns the plan schema and canonicalization algorithm. stove0 retains the complete validated plan document and its target-issued digest, but deliberately does not reinterpret or re-hash the plan with stove0's canonical JSON rules. This prevents two authorities from disagreeing about target plan identity while preserving the full document in the execution envelope.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `plan` | yes | type="object"; additional keys=`additionalProperties` |  |
| `plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `protocol` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| `target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `target_implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JsonValue](schemas-jsonvalue.md)

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

- `/external_contract/http_openapi/stove0/components/schemas/TargetPlanBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e347041141bb80657f5487a5324d1f55b68bcaee8feaa37c3c34760ba20f5045 -->

```json
{
  "additionalProperties": false,
  "description": "Opaque binding to a target-owned preflight plan.\n\nThe target protocol owns the plan schema and canonicalization algorithm. stove0\nretains the complete validated plan document and its target-issued digest, but\ndeliberately does not reinterpret or re-hash the plan with stove0's canonical\nJSON rules. This prevents two authorities from disagreeing about target plan\nidentity while preserving the full document in the execution envelope.",
  "properties": {
    "operation_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Operation Contract Sha256",
      "type": "string"
    },
    "plan": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Plan",
      "type": "object"
    },
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "protocol": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Protocol",
      "type": "string"
    },
    "target_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Target Contract Sha256",
      "type": "string"
    },
    "target_implementation_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Target Implementation Id",
      "type": "string"
    }
  },
  "required": [
    "protocol",
    "target_implementation_id",
    "target_contract_sha256",
    "operation_contract_sha256",
    "plan",
    "plan_sha256"
  ],
  "title": "TargetPlanBinding",
  "type": "object"
}
```
