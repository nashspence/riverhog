# schemas: ArchiveCopyRequestedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyrequestedevent:633035295d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-2b0f90cba9"></a>
- <a id="s-8e41c43a50"></a>`title`: ArchiveCopyRequestedEvent
- <a id="s-0e3b466f37"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4674a079ac"></a>`data` | yes | #/components/schemas/ArchiveCopyRequestedData |  |
| <a id="s-e10c913fa4"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-7e159edc1c"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-c850d02645"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-fe6c9930dc"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-b4821a8c23"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-66af1c91e7"></a>`time` | yes | type="string" |  |
| <a id="s-47849cd8a7"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy.requested" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveCopyRequestedData](schemas-archivecopyrequesteddata.md)

## Governing policies

- <a id="pa-6c0ffcc01d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRequestedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a24ee9f7531b8ef245f38422bbe1857ce1b623b1f6505ba92c4d85ec9ffafcb8 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/ArchiveCopyRequestedData"
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
      "const": "io.riverhog.riverhog.archive_copy.requested",
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
  "title": "ArchiveCopyRequestedEvent",
  "type": "object"
}
```
