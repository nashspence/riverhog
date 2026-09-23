# schemas: ArchiveCopyJobRequestedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyjobrequestedevent:c910b1cf89 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4e53477c61"></a>

- <a id="s-650c849516"></a>`type`: `"object"`
- <a id="s-d5b1f5ae50"></a>`additionalProperties`: `false`
- <a id="s-8102fa6789"></a>`required`: `["id","source","type","time","data"]`
- <a id="s-bdd91a5426"></a>`title`: `"ArchiveCopyJobRequestedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a00ff65bab"></a>`data` | yes | [ArchiveCopyJobRequestedData](schemas-archivecopyjobrequesteddata.md) |  |
| <a id="s-f0989d67f9"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json"; title="Datacontenttype" |  |
| <a id="s-0d21d128f5"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-c4919ceb82"></a>`source` | yes | type="string"; minLength=1; title="Source" |  |
| <a id="s-f3907cb2c0"></a>`specversion` | no | type="string"; const="1.0"; default="1.0"; title="Specversion" |  |
| <a id="s-6d50fd48ea"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; title="Subject" |  |
| <a id="s-16b28add54"></a>`time` | yes | type="string"; title="Time" |  |
| <a id="s-19f7983b5f"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy_job.requested"; title="Type" |  |

## Maintained corroboration

### Referenced contract elements

- [ArchiveCopyJobRequestedData](schemas-archivecopyjobrequesteddata.md)

## Governing policies

- <a id="pa-6df8f6a7d0"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobRequestedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 464b353d56d8f4268ce5b4bc21711a49e8eb8c14f43ed5cdba67c7f578295baf -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/ArchiveCopyJobRequestedData"
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
      "const": "io.riverhog.riverhog.archive_copy_job.requested",
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
  "title": "ArchiveCopyJobRequestedEvent",
  "type": "object"
}
```

</details>
