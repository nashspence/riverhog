# schemas: CollectionDeletedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectiondeletedevent:e36b3b3918 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-120961c294"></a>

- <a id="s-8e56cefa42"></a>`type`: `"object"`
- <a id="s-ceb9afa059"></a>`additionalProperties`: `false`
- <a id="s-a919cd0e30"></a>`required`: `["id","source","type","time","data"]`
- <a id="s-ac54d472d8"></a>`title`: `"CollectionDeletedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a7568f9e4d"></a>`data` | yes | [CollectionDeletedData](schemas-collectiondeleteddata.md) |  |
| <a id="s-7b787e5d73"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json"; title="Datacontenttype" |  |
| <a id="s-7dff09b524"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-07382a91d5"></a>`source` | yes | type="string"; minLength=1; title="Source" |  |
| <a id="s-8920c76575"></a>`specversion` | no | type="string"; const="1.0"; default="1.0"; title="Specversion" |  |
| <a id="s-30bba64930"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; title="Subject" |  |
| <a id="s-8c66565c5c"></a>`time` | yes | type="string"; title="Time" |  |
| <a id="s-2f5c894029"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.collection.deleted"; title="Type" |  |

## Maintained corroboration

### Referenced contract dossiers

- [CollectionDeletedData](schemas-collectiondeleteddata.md)

## Governing policies

- <a id="pa-c2b0471b07"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
