# schemas: TransformPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-transformplan:f901bc9f58 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 7 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TransformPlan`

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

- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: TargetInputAuthority](schemas-targetinputauthority.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: TransformPlan
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `inputs` | yes | #/components/schemas/TargetInputAuthority |  |
| `intent` | yes | object |  |
| `observation_result_sha256s` | no | array |  |
| `operation_contract_sha256` | yes | string |  |
| `operation_id` | yes | string |  |
| `plan_sha256` | yes | string |  |
| `protocol` | no | string |  |
| `target_contract_sha256` | yes | string |  |
| `target_implementation_id` | yes | string |  |
| `target_options` | no | object |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c8787f6bfeccc4a726d18bf765bf3296a50ab27367c6040ab9b711e1ac9ec48 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "inputs": {
      "$ref": "#/components/schemas/TargetInputAuthority"
    },
    "intent": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Intent",
      "type": "object"
    },
    "observation_result_sha256s": {
      "default": [],
      "items": {
        "pattern": "^[0-9a-f]{64}$",
        "type": "string"
      },
      "title": "Observation Result Sha256S",
      "type": "array"
    },
    "operation_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Operation Contract Sha256",
      "type": "string"
    },
    "operation_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Operation Id",
      "type": "string"
    },
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "protocol": {
      "const": "stove0-transform-target/v1",
      "default": "stove0-transform-target/v1",
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
    },
    "target_options": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Target Options",
      "type": "object"
    }
  },
  "required": [
    "operation_id",
    "operation_contract_sha256",
    "inputs",
    "intent",
    "target_implementation_id",
    "target_contract_sha256",
    "plan_sha256"
  ],
  "title": "TransformPlan",
  "type": "object"
}
```
