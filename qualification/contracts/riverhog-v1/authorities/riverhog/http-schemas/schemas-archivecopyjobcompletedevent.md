# schemas: ArchiveCopyJobCompletedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyjobcompletedevent:b5961f07e1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-a4a9b8ca39"></a>

- <a id="s-45ecbb2504"></a>`type`: `"object"`
- <a id="s-8efba89221"></a>`additionalProperties`: `false`
- <a id="s-5a2be25bfb"></a>`required`: `["id","source","type","time","data"]`
- <a id="s-872d362028"></a>`title`: `"ArchiveCopyJobCompletedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-46ed5cb698"></a>`data` | yes | [ArchiveCopyJobCompletedData](schemas-archivecopyjobcompleteddata.md) |  |
| <a id="s-c665c188ce"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json"; title="Datacontenttype" |  |
| <a id="s-db30525c05"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-a1d7b2da49"></a>`source` | yes | type="string"; minLength=1; title="Source" |  |
| <a id="s-513c4e33d2"></a>`specversion` | no | type="string"; const="1.0"; default="1.0"; title="Specversion" |  |
| <a id="s-b073fe9c25"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; title="Subject" |  |
| <a id="s-ca181321ee"></a>`time` | yes | type="string"; title="Time" |  |
| <a id="s-c15007550e"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy_job.completed"; title="Type" |  |

## Maintained corroboration

### Referenced contract elements

- [ArchiveCopyJobCompletedData](schemas-archivecopyjobcompleteddata.md)

## Governing policies

- <a id="pa-eacfc21023"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobCompletedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 009cc79da77efc0abddc02fa0ccd327193c546c35f0f4d46e73010c381417e24 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/ArchiveCopyJobCompletedData"
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
      "const": "io.riverhog.riverhog.archive_copy_job.completed",
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
  "title": "ArchiveCopyJobCompletedEvent",
  "type": "object"
}
```

</details>
