# schemas: RetrievalRequestedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalrequestedevent:b84d078e0a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b121f0c65a"></a>
- <a id="s-2252401467"></a>`title`: RetrievalRequestedEvent
- <a id="s-36b0adf5da"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cdc30bddc2"></a>`data` | yes | #/components/schemas/RetrievalRequestedData |  |
| <a id="s-cc1c959882"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-4abe85e7b4"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-ef32c95c1c"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-89504ffcf4"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-a8852514d7"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-49e4083860"></a>`time` | yes | type="string" |  |
| <a id="s-709147b851"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.requested" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RetrievalRequestedData](schemas-retrievalrequesteddata.md)

## Governing policies

- <a id="pa-5ae0915268"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalRequestedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c629adb5ba02c84b39e14a7137ae12dc14202fa22450ad2cbcf49dc83c486a9 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/RetrievalRequestedData"
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
      "const": "io.riverhog.riverhog.retrieval.requested",
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
  "title": "RetrievalRequestedEvent",
  "type": "object"
}
```
