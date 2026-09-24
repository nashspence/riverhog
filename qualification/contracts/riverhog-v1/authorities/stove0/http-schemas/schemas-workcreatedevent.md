# schemas: WorkCreatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workcreatedevent:a1cf592468 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-31286c7a21"></a>

- <a id="s-a5fac1868f"></a>`type`: `"object"`
- <a id="s-9bdeceda25"></a>`additionalProperties`: `false`
- <a id="s-c7bfcac709"></a>`required`: `["id","source","type","subject","time","data"]`
- <a id="s-3599038fec"></a>`title`: `"WorkCreatedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e5fe01cde7"></a>`data` | yes | [WorkCreatedEventData](schemas-workcreatedeventdata.md) |  |
| <a id="s-aa0945a217"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json"; title="Datacontenttype" |  |
| <a id="s-c328ad7c51"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-66e15f03ce"></a>`source` | yes | type="string"; const="urn:riverhog:stove0"; title="Source" |  |
| <a id="s-ebaa118e07"></a>`specversion` | no | type="string"; const="1.0"; default="1.0"; title="Specversion" |  |
| <a id="s-c5c0e7e820"></a>`subject` | yes | type="string"; minLength=1; title="Subject" |  |
| <a id="s-7ecc2de9c2"></a>`time` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Time" |  |
| <a id="s-0bf2ceef75"></a>`type` | yes | type="string"; const="io.riverhog.stove0.work.created"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field time](#s-7ecc2de9c2) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [WorkCreatedEventData](schemas-workcreatedeventdata.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-42202ceef3"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-0041924b53"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkCreatedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 712c3c66394745b4159368976bd1ff96566211d13e7c83521d7e9822857cf29b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/WorkCreatedEventData"
    },
    "datacontenttype": {
      "const": "application/json",
      "default": "application/json",
      "title": "Datacontenttype",
      "type": "string"
    },
    "id": {
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "source": {
      "const": "urn:riverhog:stove0",
      "title": "Source",
      "type": "string"
    },
    "specversion": {
      "const": "1.0",
      "default": "1.0",
      "title": "Specversion",
      "type": "string"
    },
    "subject": {
      "minLength": 1,
      "title": "Subject",
      "type": "string"
    },
    "time": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Time",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.stove0.work.created",
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "source",
    "type",
    "subject",
    "time",
    "data"
  ],
  "title": "WorkCreatedEvent",
  "type": "object"
}
```

</details>
