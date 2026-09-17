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
- <a id="s-59534b1416"></a>`required`: `["id","source","type","subject","time","data"]`
- <a id="s-a1f0a41d0b"></a>`title`: `"EvaluationUpdatedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fb068952fc"></a>`data` | yes | [EvaluationUpdatedEventData](schemas-evaluationupdatedeventdata.md) |  |
| <a id="s-6255211ffb"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json"; title="Datacontenttype" |  |
| <a id="s-aa52c04c52"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-639ab2ae82"></a>`source` | yes | type="string"; const="urn:riverhog:stove0"; title="Source" |  |
| <a id="s-39d587197d"></a>`specversion` | no | type="string"; const="1.0"; default="1.0"; title="Specversion" |  |
| <a id="s-5506efb30a"></a>`subject` | yes | type="string"; minLength=1; title="Subject" |  |
| <a id="s-949829661d"></a>`time` | yes | type="string"; title="Time" |  |
| <a id="s-0e615da55e"></a>`type` | yes | type="string"; const="io.riverhog.stove0.evaluation.updated"; title="Type" |  |

## Maintained corroboration

### Referenced contract elements

- [EvaluationUpdatedEventData](schemas-evaluationupdatedeventdata.md)

## Governing policies

- <a id="pa-1b8dbd7ee2"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

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

<!-- exact-contract-value: 07d0a9e7b9f687ee13a2c5aca5151461b70dff438137fae0c905683a54db77c9 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/EvaluationUpdatedEventData"
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
      "title": "Time",
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
    "source",
    "type",
    "subject",
    "time",
    "data"
  ],
  "title": "EvaluationUpdatedEvent",
  "type": "object"
}
```

</details>
