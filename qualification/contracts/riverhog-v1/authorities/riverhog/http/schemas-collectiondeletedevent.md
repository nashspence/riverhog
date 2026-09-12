# schemas: CollectionDeletedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondeletedevent:8eb9f0359b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-120961c294e3"></a>
- <a id="s-ac54d472d8c2"></a>`title`: CollectionDeletedEvent
- <a id="s-8e56cefa42ed"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a7568f9e4d27"></a>`data` | yes | #/components/schemas/CollectionDeletedData |  |
| <a id="s-7b787e5d73aa"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-7dff09b524c2"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-07382a91d51a"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-8920c76575c5"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-30bba649306a"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-8c66565c5c23"></a>`time` | yes | type="string" |  |
| <a id="s-2f5c89402998"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.collection.deleted" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionDeletedData](schemas-collectiondeleteddata.md)

## Governing policies

- <a id="pa-f18789936eff"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0faff0fbae20cadfdee4cd594bdfd683643972fef5244aba7e1d44c2d38531bd -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/CollectionDeletedData"
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
      "const": "io.riverhog.riverhog.collection.deleted",
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
  "title": "CollectionDeletedEvent",
  "type": "object"
}
```
