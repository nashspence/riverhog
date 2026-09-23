# schemas: EvaluationChildView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-evaluationchildview:26611e2cea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-c87794d161"></a>

- <a id="s-d07150de4f"></a>`type`: `"object"`
- <a id="s-90f844c731"></a>`additionalProperties`: `false`
- <a id="s-c583b95677"></a>`required`: `["variant_id","work_id","state"]`
- <a id="s-c75964838f"></a>`title`: `"EvaluationChildView"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f4f4f554f1"></a>`output` | no | anyOf=[([OutputCollectionRef](schemas-outputcollectionref.md)); (type="null")] |  |
| <a id="s-baf274864a"></a>`state` | yes | type="string"; enum=["pending","active","complete","inapplicable","failed","canceled"]; title="State" |  |
| <a id="s-28b6d88982"></a>`variant_id` | yes | type="string"; maxLength=160; minLength=1; title="Variant Id" |  |
| <a id="s-794161c858"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field variant_id](#s-28b6d88982) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field work_id](#s-794161c858) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [OutputCollectionRef](schemas-outputcollectionref.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-56f6b8548f"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-4ac1663c9e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationChildView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1092b50b2f3a5e988601a981685280dc7968d0b009e510be37e11e1b87eaab9d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "output": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/OutputCollectionRef"
        },
        {
          "type": "null"
        }
      ]
    },
    "state": {
      "enum": [
        "pending",
        "active",
        "complete",
        "inapplicable",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    },
    "variant_id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Variant Id",
      "type": "string"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "variant_id",
    "work_id",
    "state"
  ],
  "title": "EvaluationChildView",
  "type": "object"
}
```

</details>
