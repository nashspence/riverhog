# schemas: WorkflowPlanIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workflowplanintent:747bd5d627 -->

Work-independent fields that deterministically materialize a workflow plan.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: WorkflowPlanIntent
- `description`: Work-independent fields that deterministically materialize a workflow plan.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `input_retrieval_policy` | no | type="string"; enum=["available-only","allow"] |  |
| `operation` | yes | #/components/schemas/OperationRef |  |
| `output_policy` | no | type="object"; additional keys=`additionalProperties` |  |
| `requested_target_options` | no | type="object"; additional keys=`additionalProperties` |  |
| `result_kind` | no | type="string"; enum=["collection","external-effect"] |  |
| `retirement_grace_seconds` | no | type="integer"; minimum=0 |  |
| `retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"] |  |
| `target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: OperationRef](schemas-operationref.md)

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

- `/external_contract/http_openapi/stove0/components/schemas/WorkflowPlanIntent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4a242960bc5e6c8b004dd1edcfd1c9d4e5ee9b8b9bf2b7ff9e74c71118e1de37 -->

```json
{
  "additionalProperties": false,
  "description": "Work-independent fields that deterministically materialize a workflow plan.",
  "properties": {
    "input_retrieval_policy": {
      "default": "available-only",
      "enum": [
        "available-only",
        "allow"
      ],
      "title": "Input Retrieval Policy",
      "type": "string"
    },
    "operation": {
      "$ref": "#/components/schemas/OperationRef"
    },
    "output_policy": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Output Policy",
      "type": "object"
    },
    "requested_target_options": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Requested Target Options",
      "type": "object"
    },
    "result_kind": {
      "default": "collection",
      "enum": [
        "collection",
        "external-effect"
      ],
      "title": "Result Kind",
      "type": "string"
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
    },
    "target_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Target Contract Sha256",
      "type": "string"
    },
    "target_registration_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
      "title": "Target Registration Id",
      "type": "string"
    }
  },
  "required": [
    "operation",
    "target_registration_id",
    "target_contract_sha256"
  ],
  "title": "WorkflowPlanIntent",
  "type": "object"
}
```
