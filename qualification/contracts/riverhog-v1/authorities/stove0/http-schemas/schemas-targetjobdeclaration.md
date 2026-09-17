# schemas: TargetJobDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-targetjobdeclaration:0ad88fcc8c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-760a5c5306"></a>

- <a id="s-bf09248a14"></a>`type`: `"object"`
- <a id="s-57a41e0e2a"></a>`additionalProperties`: `false`
- <a id="s-de75b6bdaf"></a>`required`: `["job_id","claim_id","fence","controller_evidence","plan","workspace_assurance"]`
- <a id="s-bc26bbec89"></a>`title`: `"TargetJobDeclaration"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6237b2a272"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1; title="Claim Id" |  |
| <a id="s-06c9d16d27"></a>`controller_evidence` | yes | [ControllerEvidence](schemas-controllerevidence.md) |  |
| <a id="s-8d3f7e4ac2"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-6d4cfafa98"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Job Id" |  |
| <a id="s-4db92bdd23"></a>`plan` | yes | discriminator={"mapping":{"stove0-effect-target/v1":"#/components/schemas/EffectPlan","stove0-transform-target/v1":"#/components/schemas/TransformPlan"},"propertyName":"protocol"}; oneOf=[([TransformPlan](schemas-transformplan.md)); ([EffectPlan](schemas-effectplan.md))]; title="Plan" |  |
| <a id="s-c03dc9589f"></a>`workspace_assurance` | yes | type="string"; enum=["encrypted","ephemeral"]; title="Workspace Assurance" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-6237b2a272) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field job_id](#s-6d4cfafa98) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [ControllerEvidence](schemas-controllerevidence.md)
- [EffectPlan](schemas-effectplan.md)
- [TransformPlan](schemas-transformplan.md)

## Governing policies

- <a id="pa-d647585316"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-ab26737f80"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetJobDeclaration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
