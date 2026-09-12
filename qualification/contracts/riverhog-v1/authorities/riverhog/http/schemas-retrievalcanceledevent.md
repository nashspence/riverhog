# schemas: RetrievalCanceledEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalcanceledevent:0e0118c6ae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-80b70fb0b852"></a>
- <a id="s-6221c4174bef"></a>`title`: RetrievalCanceledEvent
- <a id="s-9ffc2ad2da6a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c6658ffee676"></a>`data` | yes | #/components/schemas/RetrievalCanceledData |  |
| <a id="s-d2b18bd4a5f0"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-476f0632b505"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-7da3146f347c"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-ff397a748894"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-1b2315cd8347"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-484880631ad0"></a>`time` | yes | type="string" |  |
| <a id="s-186330c03ad6"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.canceled" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RetrievalCanceledData](schemas-retrievalcanceleddata.md)

## Governing policies

- <a id="pa-1a9233f212eb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCanceledEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad60c3c6616ced4b0e974819569f76b7d31c36d32e48c2f7a3062826de5efa15 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/RetrievalCanceledData"
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
      "const": "io.riverhog.riverhog.retrieval.canceled",
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
  "title": "RetrievalCanceledEvent",
  "type": "object"
}
```
