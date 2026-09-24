# schemas: PreviewTargetExpectationView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-previewtargetexpectationview:a09b41c33d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-9d02d3705b"></a>

- <a id="s-d8cc2c8747"></a>`type`: `"object"`
- <a id="s-922c47a74c"></a>`additionalProperties`: `false`
- <a id="s-2f76e2c210"></a>`required`: `["branch_id","work_id","plan_sha256"]`
- <a id="s-e04016021d"></a>`title`: `"PreviewTargetExpectationView"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4da8083975"></a>`branch_id` | yes | type="string"; maxLength=160; minLength=1; title="Branch Id" |  |
| <a id="s-cccc57ac09"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-bfcb19dacf"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_id](#s-4da8083975) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field plan_sha256](#s-cccc57ac09) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field work_id](#s-bfcb19dacf) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-dfaf5de1a9"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-abebfe61b6"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/PreviewTargetExpectationView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 581425e1055d7eec18f490cb5b2d2a5831a8516dd03460d4c159ee3743e6df34 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "branch_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Branch Id",
      "type": "string"
    },
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "branch_id",
    "work_id",
    "plan_sha256"
  ],
  "title": "PreviewTargetExpectationView",
  "type": "object"
}
```

</details>
