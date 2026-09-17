# schemas: RetrievalIssueEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalissueevent:0af8149e67 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-39ba081052"></a>

- <a id="s-b499cfd260"></a>`type`: `"object"`
- <a id="s-79ab560e71"></a>`additionalProperties`: `false`
- <a id="s-6c6cc4506f"></a>`required`: `["id","source","type","time","data"]`
- <a id="s-c9688ae39e"></a>`title`: `"RetrievalIssueEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed3ecdd6cc"></a>`data` | yes | [RetrievalIssueData](schemas-retrievalissuedata.md) |  |
| <a id="s-a30d8d6a01"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json"; title="Datacontenttype" |  |
| <a id="s-4395d57bb8"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-90d4c9a8c7"></a>`source` | yes | type="string"; minLength=1; title="Source" |  |
| <a id="s-415f6d1727"></a>`specversion` | no | type="string"; const="1.0"; default="1.0"; title="Specversion" |  |
| <a id="s-840c8c2eb5"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; title="Subject" |  |
| <a id="s-63d40366db"></a>`time` | yes | type="string"; title="Time" |  |
| <a id="s-7346fa5b71"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.issue"; title="Type" |  |

## Maintained corroboration

### Referenced contract dossiers

- [RetrievalIssueData](schemas-retrievalissuedata.md)

## Governing policies

- <a id="pa-7744576454"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalIssueEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a47a3eb99b6baa5403973df94bb32789973c413ad2a08f90804ed2b7da7a404 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/RetrievalIssueData"
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
      "const": "io.riverhog.riverhog.retrieval.issue",
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
  "title": "RetrievalIssueEvent",
  "type": "object"
}
```

</details>
