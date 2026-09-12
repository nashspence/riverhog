# schemas: EvaluationCreatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationcreatedevent:52e126d3dd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-cf1dcf0e2397"></a>
- <a id="s-865750d0df25"></a>`title`: EvaluationCreatedEvent
- <a id="s-473729b35a27"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-538b5ca99f1e"></a>`data` | yes | #/components/schemas/EvaluationCreatedEventData |  |
| <a id="s-60c3ea799c69"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-f51b84652689"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-d16f00f0b178"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-5c37f71fa162"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-12e5c43b39e5"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-04b276da4ae8"></a>`time` | yes | type="string" |  |
| <a id="s-03c52a6409a2"></a>`type` | yes | type="string"; const="io.riverhog.stove0.evaluation.created" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: EvaluationCreatedEventData](schemas-evaluationcreatedeventdata.md)

## Governing policies

- <a id="pa-48be4a40e130"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationCreatedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e9f4023a06d936d53b69cbb92aa15c70781eed06c2c9c6322a3084b9e1b4339 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/EvaluationCreatedEventData"
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
      "const": "io.riverhog.stove0.evaluation.created",
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
  "title": "EvaluationCreatedEvent",
  "type": "object"
}
```
