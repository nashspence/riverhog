# schemas: WorkCreatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workcreatedevent:c2aac6ffd5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-31286c7a2168"></a>
- <a id="s-3599038fecd9"></a>`title`: WorkCreatedEvent
- <a id="s-a5fac1868fd4"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e5fe01cde776"></a>`data` | yes | #/components/schemas/WorkCreatedEventData |  |
| <a id="s-aa0945a2171a"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-c328ad7c51ab"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-66e15f03ce83"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-ebaa118e07c3"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-c5c0e7e82011"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-7ecc2de9c2e6"></a>`time` | yes | type="string" |  |
| <a id="s-0bf2ceef7591"></a>`type` | yes | type="string"; const="io.riverhog.stove0.work.created" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: WorkCreatedEventData](schemas-workcreatedeventdata.md)

## Governing policies

- <a id="pa-4030e3755171"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkCreatedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce2e425259092591caa0366c005e5359c521121ab027ab0aaf7e66639495eb66 -->

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
