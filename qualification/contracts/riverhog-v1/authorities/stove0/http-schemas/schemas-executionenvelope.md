# schemas: ExecutionEnvelope

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-executionenvelope:079fcc3e53 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-fc88d5a952"></a>

- <a id="s-abdee4f349"></a>`type`: `"object"`
- <a id="s-b3d876118e"></a>`additionalProperties`: `false`
- <a id="s-4d6c0171ac"></a>`required`: `["claim_id","fence","workflow_plan","target_plan","execution_envelope_sha256"]`
- <a id="s-d493c8c79f"></a>`title`: `"ExecutionEnvelope"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-254db14200"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-7722433db7"></a>`execution_envelope_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Envelope Sha256" |  |
| <a id="s-5debb009eb"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-7e6d8ecf9e"></a>`format` | no | type="string"; const="stove0-execution-envelope/v1"; default="stove0-execution-envelope/v1"; title="Format" |  |
| <a id="s-b47a72301b"></a>`target_plan` | yes | [TargetPlanBinding](schemas-targetplanbinding.md) |  |
| <a id="s-3d863bfbd7"></a>`workflow_plan` | yes | [WorkflowPlan](schemas-workflowplan.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-254db14200) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field execution_envelope_sha256](#s-7722433db7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [TargetPlanBinding](schemas-targetplanbinding.md)
- [WorkflowPlan](schemas-workflowplan.md)

## Governing policies

- <a id="pa-0270edf8fd"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-dd15c8bd38"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ExecutionEnvelope`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
