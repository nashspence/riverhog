# schemas: CollectionFinalizedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionfinalizedevent:08d1c05b50 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6326a0a9a085"></a>
- <a id="s-0c2325ff42d4"></a>`title`: CollectionFinalizedEvent
- <a id="s-9976252508f0"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e7e5352fcee4"></a>`data` | yes | #/components/schemas/CollectionFinalizedData |  |
| <a id="s-140b4a01cd72"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-3f1ca51c8549"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-6cbe501200e4"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-cfd6dac66173"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-f398c16b138c"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-62d816a63d63"></a>`time` | yes | type="string" |  |
| <a id="s-f9b2dd95c2a9"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.collection.finalized" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionFinalizedData](schemas-collectionfinalizeddata.md)

## Governing policies

- <a id="pa-b7821304de83"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionFinalizedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ec43406f4b6ab857cd8093f0789753d7463d2b04ec1f088aa8b54222bc62a61 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/CollectionFinalizedData"
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
      "const": "io.riverhog.riverhog.collection.finalized",
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
  "title": "CollectionFinalizedEvent",
  "type": "object"
}
```
