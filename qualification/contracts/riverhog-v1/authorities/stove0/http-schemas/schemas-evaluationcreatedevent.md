# schemas: EvaluationCreatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-evaluationcreatedevent:b63e3dd6d5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-cf1dcf0e23"></a>

- <a id="s-473729b35a"></a>`type`: `"object"`
- <a id="s-1d987b7e1f"></a>`additionalProperties`: `false`
- <a id="s-10ea9b0981"></a>`required`: `["id","type","subject","occurred_at","payload"]`
- <a id="s-865750d0df"></a>`title`: `"EvaluationCreatedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f51b846526"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-959b33249d"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Occurred At" |  |
| <a id="s-9611926f56"></a>`payload` | yes | [EvaluationCreatedEventData](schemas-evaluationcreatedeventdata.md) |  |
| <a id="s-12e5c43b39"></a>`subject` | yes | type="string"; minLength=1; title="Subject" |  |
| <a id="s-03c52a6409"></a>`type` | yes | type="string"; const="io.riverhog.stove0.evaluation.created"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field occurred_at](#s-959b33249d) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [EvaluationCreatedEventData](schemas-evaluationcreatedeventdata.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-dec33db1f5"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-73e0735687"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationCreatedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d433ddd885081bcfa0aa962e8cf843681fc500e5d0a29c6c6c432abe364728aa -->

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
      "$ref": "#/components/schemas/EvaluationCreatedEventData"
    },
    "subject": {
      "minLength": 1,
      "title": "Subject",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.stove0.evaluation.created",
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
  "title": "EvaluationCreatedEvent",
  "type": "object"
}
```

</details>
