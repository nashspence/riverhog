# schemas: ArchiveCopyJobCanceledEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyjobcanceledevent:dce99c4668 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4a12430732"></a>

- <a id="s-ea86e78495"></a>`type`: `"object"`
- <a id="s-d51bd8b0e3"></a>`additionalProperties`: `false`
- <a id="s-91b21926bb"></a>`required`: `["id","source","type","time","data"]`
- <a id="s-64bf4fcd79"></a>`title`: `"ArchiveCopyJobCanceledEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d6d27a8c42"></a>`data` | yes | [ArchiveCopyJobCanceledData](schemas-archivecopyjobcanceleddata.md) |  |
| <a id="s-a7e722f364"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json"; title="Datacontenttype" |  |
| <a id="s-9e0d8dfed2"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-8eaec7f5bf"></a>`source` | yes | type="string"; minLength=1; title="Source" |  |
| <a id="s-c584d05be9"></a>`specversion` | no | type="string"; const="1.0"; default="1.0"; title="Specversion" |  |
| <a id="s-6e2d40fb72"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; title="Subject" |  |
| <a id="s-6bac22ebae"></a>`time` | yes | type="string"; title="Time" |  |
| <a id="s-42992ec837"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy_job.canceled"; title="Type" |  |

## Maintained corroboration

### Referenced contract elements

- [ArchiveCopyJobCanceledData](schemas-archivecopyjobcanceleddata.md)

## Governing policies

- <a id="pa-a4405fad8d"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobCanceledEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b5138f032e13aa87360417132a9ccb431400f6b0bd5e31e656701977bcc4cf3 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/ArchiveCopyJobCanceledData"
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
      "const": "io.riverhog.riverhog.archive_copy_job.canceled",
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
  "title": "ArchiveCopyJobCanceledEvent",
  "type": "object"
}
```

</details>
