# schemas: WorkUpdatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workupdatedevent:92bb1049cd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-46805a6e033c"></a>
- <a id="s-b3978166b280"></a>`title`: WorkUpdatedEvent
- <a id="s-4a84b11889b7"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-089d96c784ae"></a>`data` | yes | #/components/schemas/WorkUpdatedEventData |  |
| <a id="s-0a458eb822b6"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-5445b0c1bba6"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-dce1579c51e2"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-e4a4e26b1028"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-7d62dec9935b"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-c6fb4b73c237"></a>`time` | yes | type="string" |  |
| <a id="s-eea0cd0fd210"></a>`type` | yes | type="string"; const="io.riverhog.stove0.work.updated" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: WorkUpdatedEventData](schemas-workupdatedeventdata.md)

## Governing policies

- <a id="pa-f031f34ae37a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkUpdatedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a1b6b1d4b060d43dbd4be5aa2a5643cda62c6dfee7e2badf04ca9d2bbaf89a76 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/WorkUpdatedEventData"
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
      "const": "io.riverhog.stove0.work.updated",
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
  "title": "WorkUpdatedEvent",
  "type": "object"
}
```
