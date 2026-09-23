# schemas: EvaluationCreatedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-evaluationcreatedeventdata:e7589be1d7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f5df070c91"></a>

- <a id="s-f0522c80d4"></a>`type`: `"object"`
- <a id="s-9a3f0ecf9a"></a>`additionalProperties`: `false`
- <a id="s-dc5132732c"></a>`required`: `["evaluation_id","phase"]`
- <a id="s-31608481e3"></a>`title`: `"EvaluationCreatedEventData"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-43677855e7"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Evaluation Id" |  |
| <a id="s-86a49fe56f"></a>`phase` | yes | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"]; title="Phase" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field evaluation_id](#s-43677855e7) | `length · characters · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6d58b3ba9f"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e776bffaeb"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationCreatedEventData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8a95a196acf8a76a51c6b3ef5fe4f5abffb52508d5830fac6aac0ff2138ae10a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "evaluation_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Evaluation Id",
      "type": "string"
    },
    "phase": {
      "enum": [
        "planning",
        "running",
        "partially_complete",
        "complete",
        "failed",
        "canceled"
      ],
      "title": "Phase",
      "type": "string"
    }
  },
  "required": [
    "evaluation_id",
    "phase"
  ],
  "title": "EvaluationCreatedEventData",
  "type": "object"
}
```

</details>
