# schemas: EvaluationUpdatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-evaluationupdatedevent:798c573533 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-87e822cefa"></a>

- <a id="s-e51f9d65a9"></a>`type`: `"object"`
- <a id="s-e177f13c0f"></a>`additionalProperties`: `false`
- <a id="s-59534b1416"></a>`required`: `["id","type","subject","occurred_at","payload"]`
- <a id="s-a1f0a41d0b"></a>`title`: `"EvaluationUpdatedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aa52c04c52"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-1d611e51ff"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Occurred At" |  |
| <a id="s-ea7757db93"></a>`payload` | yes | [EvaluationUpdatedEventData](schemas-evaluationupdatedeventdata.md) |  |
| <a id="s-5506efb30a"></a>`subject` | yes | type="string"; minLength=1; title="Subject" |  |
| <a id="s-0e615da55e"></a>`type` | yes | type="string"; const="io.riverhog.stove0.evaluation.updated"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field occurred_at](#s-1d611e51ff) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [EvaluationUpdatedEventData](schemas-evaluationupdatedeventdata.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1b8dbd7ee2"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-0bc8a94680"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationUpdatedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d060d5e30c8c7226aedf3a3f55711d0bea5b42837247bd393163530976cdcdc -->

```json
{
  "additionalProperties": false,
  "properties": {
    "id": {
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "occurred_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Occurred At",
      "type": "string"
    },
    "payload": {
      "$ref": "#/components/schemas/EvaluationUpdatedEventData"
    },
    "subject": {
      "minLength": 1,
      "title": "Subject",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.stove0.evaluation.updated",
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "type",
    "subject",
    "occurred_at",
    "payload"
  ],
  "title": "EvaluationUpdatedEvent",
  "type": "object"
}
```

</details>
