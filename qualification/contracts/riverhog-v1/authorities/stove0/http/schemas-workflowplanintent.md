# schemas: WorkflowPlanIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workflowplanintent:747bd5d627 -->

Work-independent fields that deterministically materialize a workflow plan.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-7e51d1430dea"></a>
- <a id="s-23c8e189de8a"></a>`title`: WorkflowPlanIntent
- <a id="s-d32c77c5685f"></a>`description`: Work-independent fields that deterministically materialize a workflow plan.
- <a id="s-642b2b27631f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fc12db4ac7b2"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"] |  |
| <a id="s-dec7652768d7"></a>`operation` | yes | #/components/schemas/OperationRef |  |
| <a id="s-99bb65af4851"></a>`output_policy` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-0bb17d7fffeb"></a>`requested_target_options` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-37b4b891758d"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"] |  |
| <a id="s-e519a846125f"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0 |  |
| <a id="s-70ece6e07d43"></a>`retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"] |  |
| <a id="s-299b5608b2bb"></a>`target_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-cabd6d9c4eae"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field output_policy](#s-99bb65af4851) | `cardinality · entries · operational_policy` | shared above |
| [field requested_target_options](#s-0bb17d7fffeb) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field target_contract_sha256](#s-299b5608b2bb) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: OperationRef](schemas-operationref.md)

## Governing policies

- <a id="pa-e4b5c057355f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-263a4dcdcde1"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-1316c7b324c9"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
