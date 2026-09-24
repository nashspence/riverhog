# schemas: WorkUpdatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workupdatedevent:4bfc4e0ba1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-46805a6e03"></a>

- <a id="s-4a84b11889"></a>`type`: `"object"`
- <a id="s-c2f1d620dc"></a>`additionalProperties`: `false`
- <a id="s-077330d907"></a>`required`: `["id","type","subject","occurred_at","payload"]`
- <a id="s-b3978166b2"></a>`title`: `"WorkUpdatedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5445b0c1bb"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-412414a006"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Occurred At" |  |
| <a id="s-0d82f3e95b"></a>`payload` | yes | [WorkUpdatedEventData](schemas-workupdatedeventdata.md) |  |
| <a id="s-7d62dec993"></a>`subject` | yes | type="string"; minLength=1; title="Subject" |  |
| <a id="s-eea0cd0fd2"></a>`type` | yes | type="string"; const="io.riverhog.stove0.work.updated"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field occurred_at](#s-412414a006) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [WorkUpdatedEventData](schemas-workupdatedeventdata.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-7629c4b3cd"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-8173d7d726"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkUpdatedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de1535c538c849ebc55fa409c6583ed8eec14948ad48bdfe78b81a3481be2746 -->

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
      "$ref": "#/components/schemas/WorkUpdatedEventData"
    },
    "subject": {
      "minLength": 1,
      "title": "Subject",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.stove0.work.updated",
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
  "title": "WorkUpdatedEvent",
  "type": "object"
}
```

</details>
