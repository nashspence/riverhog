# schemas: WorkflowPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workflowplan:8ac5e095fa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-5da2fc67106f"></a>
- <a id="s-d1846f1c20f5"></a>`title`: WorkflowPlan
- <a id="s-c51e7ceed05a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6b03446110dd"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1" |  |
| <a id="s-6ad1a7b068da"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"] |  |
| <a id="s-6ef6cec03dd7"></a>`observations` | no | type="array"; items=(#/components/schemas/ObservationEvidence) |  |
| <a id="s-5cabb9dd3b67"></a>`operation` | yes | #/components/schemas/OperationRef |  |
| <a id="s-bf4f29710f28"></a>`output_policy` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-a5d912ee095f"></a>`requested_target_options` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-f570ec6b980c"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"] |  |
| <a id="s-dd4a7b6bed98"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0 |  |
| <a id="s-d77e80789747"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"] |  |
| <a id="s-d12146eea6db"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8e91339dcf5a"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |
| <a id="s-28aa8c8face7"></a>`work` | yes | #/components/schemas/WorkIdentity |  |
| <a id="s-212a10b34a7d"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field observations](#s-6ef6cec03dd7) | `cardinality · items · operational_policy` | shared above |
| [field output_policy](#s-bf4f29710f28) | `cardinality · entries · operational_policy` | shared above |
| [field requested_target_options](#s-a5d912ee095f) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field target_contract_sha256](#s-d12146eea6db) | `length · characters · fixed` | shared above |
| [field workflow_plan_sha256](#s-212a10b34a7d) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: ObservationEvidence](schemas-observationevidence.md)
- [schemas: OperationRef](schemas-operationref.md)
- [schemas: WorkIdentity](schemas-workidentity.md)

## Governing policies

- <a id="pa-ffb4973e6b75"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-51564e5b6d91"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-9335106b690a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkflowPlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1ac28da8e579dbffb0ead200dbc696f5253dae3438526682093a5f3e85c9c7bf -->

```json
{
  "additionalProperties": false,
  "properties": {
    "format": {
      "const": "stove0-workflow-plan/v1",
      "default": "stove0-workflow-plan/v1",
      "title": "Format",
      "type": "string"
    },
    "input_retrieval_policy": {
      "default": "available-only",
      "enum": [
        "available-only",
        "allow"
      ],
      "title": "Input Retrieval Policy",
      "type": "string"
    },
    "observations": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/ObservationEvidence"
      },
      "title": "Observations",
      "type": "array"
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
    },
    "work": {
      "$ref": "#/components/schemas/WorkIdentity"
    },
    "workflow_plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Workflow Plan Sha256",
      "type": "string"
    }
  },
  "required": [
    "work",
    "operation",
    "target_registration_id",
    "target_contract_sha256",
    "workflow_plan_sha256"
  ],
  "title": "WorkflowPlan",
  "type": "object"
}
```
