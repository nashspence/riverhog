# schemas: RetrievalReadyEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalreadyevent:acee1f90c9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-38d6ad8439"></a>
- <a id="s-de37f7a8cd"></a>`title`: RetrievalReadyEvent
- <a id="s-c3c03e1023"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-68122ab8cd"></a>`data` | yes | #/components/schemas/RetrievalReadyData |  |
| <a id="s-439f04025d"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-b4a383aa60"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-bf46e50353"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-7bad14c84c"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-d45f3e80da"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-41e929a32e"></a>`time` | yes | type="string" |  |
| <a id="s-9b5ee6830d"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.ready" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RetrievalReadyData](schemas-retrievalreadydata.md)

## Governing policies

- <a id="pa-b7867912fe"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalReadyEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 94095da7d1d8864089fbf473876cb59cb189bb82c5c09d01631b94c2de3ec30a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/RetrievalReadyData"
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
      "const": "io.riverhog.riverhog.retrieval.ready",
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
  "title": "RetrievalReadyEvent",
  "type": "object"
}
```
