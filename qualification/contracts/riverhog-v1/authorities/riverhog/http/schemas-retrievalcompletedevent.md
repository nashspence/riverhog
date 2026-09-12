# schemas: RetrievalCompletedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcompletedevent:3f6c7ef001 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a49d05edb163"></a>
- <a id="s-c6f1ce75a408"></a>`title`: RetrievalCompletedEvent
- <a id="s-750a8db6261a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2a0f68c88aa7"></a>`data` | yes | #/components/schemas/RetrievalCompletedData |  |
| <a id="s-d702cfb5bf09"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-c4fea1bccd3c"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-d8681b0ed9a6"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-aa88737e5510"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-e0e0a51d71a4"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-15b34c1a4523"></a>`time` | yes | type="string" |  |
| <a id="s-ca867fdf3cd7"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.completed" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RetrievalCompletedData](schemas-retrievalcompleteddata.md)

## Governing policies

- <a id="pa-0c0461b7562a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCompletedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 435139366870d8b25f95ceb73edc6edecd1daf7486713f7713771bfbb81a4ee0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/RetrievalCompletedData"
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
      "minLength": 1,
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
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Subject"
    },
    "time": {
      "title": "Time",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.riverhog.retrieval.completed",
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "source",
    "type",
    "time",
    "data"
  ],
  "title": "RetrievalCompletedEvent",
  "type": "object"
}
```
