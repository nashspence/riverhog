# schemas: JoinAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-joinadmittedevent:106712145f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-89c2b9e530"></a>

- <a id="s-797a81b125"></a>`type`: `"object"`
- <a id="s-e6f1ea9542"></a>`additionalProperties`: `false`
- <a id="s-d719486eb3"></a>`required`: `["id","type","subject","occurred_at","payload"]`
- <a id="s-b5df77a5be"></a>`title`: `"JoinAdmittedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3adf064b80"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-d057424438"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Occurred At" |  |
| <a id="s-1cb8fcff00"></a>`payload` | yes | [JoinAdmittedEventData](schemas-joinadmittedeventdata.md) |  |
| <a id="s-ae8b6da303"></a>`subject` | yes | type="string"; minLength=1; title="Subject" |  |
| <a id="s-dbf4280eb9"></a>`type` | yes | type="string"; const="io.riverhog.stove0.join.admitted"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field occurred_at](#s-d057424438) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [JoinAdmittedEventData](schemas-joinadmittedeventdata.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-eda5c5b206"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-ca39938afb"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinAdmittedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a145c2ecfc112d37552dd1a9ed24502ca1357f542f1f72e8fa403c4d790edf26 -->

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
      "$ref": "#/components/schemas/JoinAdmittedEventData"
    },
    "subject": {
      "minLength": 1,
      "title": "Subject",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.stove0.join.admitted",
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
  "title": "JoinAdmittedEvent",
  "type": "object"
}
```

</details>
