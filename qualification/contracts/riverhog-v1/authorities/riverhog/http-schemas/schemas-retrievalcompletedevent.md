# schemas: RetrievalCompletedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalcompletedevent:c0fc37f8a2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-a49d05edb1"></a>

- <a id="s-750a8db626"></a>`type`: `"object"`
- <a id="s-5e25b7989a"></a>`additionalProperties`: `false`
- <a id="s-b5782174c0"></a>`required`: `["id","source","type","time","data"]`
- <a id="s-c6f1ce75a4"></a>`title`: `"RetrievalCompletedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2a0f68c88a"></a>`data` | yes | [RetrievalCompletedData](schemas-retrievalcompleteddata.md) |  |
| <a id="s-d702cfb5bf"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json"; title="Datacontenttype" |  |
| <a id="s-c4fea1bccd"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-d8681b0ed9"></a>`source` | yes | type="string"; minLength=1; title="Source" |  |
| <a id="s-aa88737e55"></a>`specversion` | no | type="string"; const="1.0"; default="1.0"; title="Specversion" |  |
| <a id="s-e0e0a51d71"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; title="Subject" |  |
| <a id="s-15b34c1a45"></a>`time` | yes | type="string"; title="Time" |  |
| <a id="s-ca867fdf3c"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.completed"; title="Type" |  |

## Maintained corroboration

### Referenced contract dossiers

- [RetrievalCompletedData](schemas-retrievalcompleteddata.md)

## Governing policies

- <a id="pa-cbb5014cb6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCompletedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
