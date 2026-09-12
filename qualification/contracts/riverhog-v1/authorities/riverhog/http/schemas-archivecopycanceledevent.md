# schemas: ArchiveCopyCanceledEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopycanceledevent:1bc30c355a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-2e52b0b65a77"></a>
- <a id="s-cfcca6c665c2"></a>`title`: ArchiveCopyCanceledEvent
- <a id="s-1abd81344dfa"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e44ec5f85548"></a>`data` | yes | #/components/schemas/ArchiveCopyCanceledData |  |
| <a id="s-2f8d64ea42b4"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-80d1b94a6e94"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-542168d2d28d"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-14e6d1cf9fdc"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-f1e11a5cc625"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-44568135ec21"></a>`time` | yes | type="string" |  |
| <a id="s-611d36be0d59"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy.canceled" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveCopyCanceledData](schemas-archivecopycanceleddata.md)

## Governing policies

- <a id="pa-f22463d5ffe2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyCanceledEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f502d86f2bd7d13726f47a47bbc60bddae62706b3613dd2c1f162d904246e59d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/ArchiveCopyCanceledData"
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
      "const": "io.riverhog.riverhog.archive_copy.canceled",
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
  "title": "ArchiveCopyCanceledEvent",
  "type": "object"
}
```
