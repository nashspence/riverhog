# schemas: EvaluationUpdatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-evaluationupdatedevent:3e0258eb84 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-87e822cefa25"></a>
- <a id="s-a1f0a41d0b31"></a>`title`: EvaluationUpdatedEvent
- <a id="s-e51f9d65a971"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fb068952fc85"></a>`data` | yes | #/components/schemas/EvaluationUpdatedEventData |  |
| <a id="s-6255211ffb31"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-aa52c04c529a"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-639ab2ae8287"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-39d587197ddf"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-5506efb30a9a"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-949829661d18"></a>`time` | yes | type="string" |  |
| <a id="s-0e615da55ebd"></a>`type` | yes | type="string"; const="io.riverhog.stove0.evaluation.updated" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: EvaluationUpdatedEventData](schemas-evaluationupdatedeventdata.md)

## Governing policies

- <a id="pa-90427561f353"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/EvaluationUpdatedEvent`

### Exact owned JSON

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
