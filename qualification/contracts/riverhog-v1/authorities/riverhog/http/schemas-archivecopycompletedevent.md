# schemas: ArchiveCopyCompletedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopycompletedevent:3105f4ff28 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-fc5764194a81"></a>
- <a id="s-74d19a88e4e5"></a>`title`: ArchiveCopyCompletedEvent
- <a id="s-22dae3a0f258"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-16cdd26fdb91"></a>`data` | yes | #/components/schemas/ArchiveCopyCompletedData |  |
| <a id="s-e859482fc6a0"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-743f631cd391"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-567841d8c7ba"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-4bac774fbaa5"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-fb4c268606b4"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-cd4f22b7288a"></a>`time` | yes | type="string" |  |
| <a id="s-051108df5e39"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy.completed" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveCopyCompletedData](schemas-archivecopycompleteddata.md)

## Governing policies

- <a id="pa-b4265690e7cf"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyCompletedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 936c18c949d27245bbe242cc88117a23e86f1db81ae7e9d77f4563f917b94660 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/ArchiveCopyCompletedData"
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
      "const": "io.riverhog.riverhog.archive_copy.completed",
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
  "title": "ArchiveCopyCompletedEvent",
  "type": "object"
}
```
