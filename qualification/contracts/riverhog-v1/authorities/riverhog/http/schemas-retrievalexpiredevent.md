# schemas: RetrievalExpiredEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalexpiredevent:81f9c9c8a0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e102080776"></a>
- <a id="s-2e46c614ff"></a>`title`: RetrievalExpiredEvent
- <a id="s-83e3b5fdb2"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a70e65c3c7"></a>`data` | yes | #/components/schemas/RetrievalExpiredData |  |
| <a id="s-ed4b356122"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-1f4f2d7d08"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-2e6997fde9"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-f69ac13250"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-4dadef44a7"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-3bc6cf0fd9"></a>`time` | yes | type="string" |  |
| <a id="s-4d9f982b0f"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.expired" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RetrievalExpiredData](schemas-retrievalexpireddata.md)

## Governing policies

- <a id="pa-bf0ae04df2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalExpiredEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 412430ca84d67b6e95467c644afdd9da464beeab227bfe52f0d86b6f0a3a60cc -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/RetrievalExpiredData"
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
      "const": "io.riverhog.riverhog.retrieval.expired",
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
  "title": "RetrievalExpiredEvent",
  "type": "object"
}
```
