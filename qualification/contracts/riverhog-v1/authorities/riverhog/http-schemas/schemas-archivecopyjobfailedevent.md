# schemas: ArchiveCopyJobFailedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyjobfailedevent:41878fbc47 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-afda7a1b70"></a>

- <a id="s-eb5544fb20"></a>`type`: `"object"`
- <a id="s-d7bc97eb46"></a>`additionalProperties`: `false`
- <a id="s-6c9dd10487"></a>`required`: `["id","source","type","time","data"]`
- <a id="s-96920da11b"></a>`title`: `"ArchiveCopyJobFailedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3f8c6c5456"></a>`data` | yes | [ArchiveCopyJobFailedData](schemas-archivecopyjobfaileddata.md) |  |
| <a id="s-e1b7072045"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json"; title="Datacontenttype" |  |
| <a id="s-4613dced05"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-26e7b47a8a"></a>`source` | yes | type="string"; minLength=1; title="Source" |  |
| <a id="s-75846b4990"></a>`specversion` | no | type="string"; const="1.0"; default="1.0"; title="Specversion" |  |
| <a id="s-fbe352fbc9"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; title="Subject" |  |
| <a id="s-ac9dc45347"></a>`time` | yes | type="string"; title="Time" |  |
| <a id="s-3d72924d38"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy_job.failed"; title="Type" |  |

## Maintained corroboration

### Referenced contract elements

- [ArchiveCopyJobFailedData](schemas-archivecopyjobfaileddata.md)

## Governing policies

- <a id="pa-4a7ac7781c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobFailedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d7d1d9bf02b0f5b47c68fe041d2800016f38005ca37a641112bb46e7c22184c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/ArchiveCopyJobFailedData"
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
      "const": "io.riverhog.riverhog.archive_copy_job.failed",
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
  "title": "ArchiveCopyJobFailedEvent",
  "type": "object"
}
```

</details>
