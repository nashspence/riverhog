# schemas: ExecutionEnvelope

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-executionenvelope:610b04b926 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-fc88d5a952"></a>
- <a id="s-d493c8c79f"></a>`title`: ExecutionEnvelope
- <a id="s-abdee4f349"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-254db14200"></a>`claim_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-7722433db7"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5debb009eb"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-7e6d8ecf9e"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1" |  |
| <a id="s-b47a72301b"></a>`target_plan` | yes | #/components/schemas/TargetPlanBinding |  |
| <a id="s-3d863bfbd7"></a>`workflow_plan` | yes | #/components/schemas/WorkflowPlan |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-254db14200) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field execution_envelope_sha256](#s-7722433db7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: TargetPlanBinding](schemas-targetplanbinding.md)
- [schemas: WorkflowPlan](schemas-workflowplan.md)

## Governing policies

- <a id="pa-9e2113fbc3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-380b6ac673"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ExecutionEnvelope`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4fb5bd658e35492c990cbae657cbfe2c6bc098a5f9346c50a331ebc112d3ae41 -->

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
    "execution_envelope_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Execution Envelope Sha256",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "format": {
      "const": "stove0-execution-envelope/v1",
      "default": "stove0-execution-envelope/v1",
      "title": "Format",
      "type": "string"
    },
    "target_plan": {
      "$ref": "#/components/schemas/TargetPlanBinding"
    },
    "workflow_plan": {
      "$ref": "#/components/schemas/WorkflowPlan"
    }
  },
  "required": [
    "claim_id",
    "fence",
    "workflow_plan",
    "target_plan",
    "execution_envelope_sha256"
  ],
  "title": "ExecutionEnvelope",
  "type": "object"
}
```
