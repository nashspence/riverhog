# schemas: RetrievalFailedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalfailedevent:284f8534c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0c63f79edb"></a>
- <a id="s-d7a1b8c167"></a>`title`: RetrievalFailedEvent
- <a id="s-7bdd13aadb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a3043da3e"></a>`data` | yes | #/components/schemas/RetrievalFailedData |  |
| <a id="s-18a9dd8d69"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-6bee5ce69d"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-0189802cc0"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-3b028c5774"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-0c9259d07a"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-ebbc8d7562"></a>`time` | yes | type="string" |  |
| <a id="s-a50bef74c4"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.failed" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RetrievalFailedData](schemas-retrievalfaileddata.md)

## Governing policies

- <a id="pa-84cc1e0fe6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalFailedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1397b8a710aa9838228bc4fab62b9a367326c9ded19fdad8a8a5c345c1945d66 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/RetrievalFailedData"
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
      "const": "io.riverhog.riverhog.retrieval.failed",
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
  "title": "RetrievalFailedEvent",
  "type": "object"
}
```
