# schemas: FailedArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-failedarchivecopyout:069daa6dc4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a43ee22d2f"></a>
- <a id="s-b9348fc110"></a>`title`: FailedArchiveCopyOut
- <a id="s-cd5a47e8d6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4b8104d18d"></a>`archive_root` | yes | #/components/schemas/FailedArchiveRootPublicationOut |  |
| <a id="s-3cff6a3fed"></a>`failure` | yes | type="string"; minLength=1 |  |
| <a id="s-118170bffa"></a>`last_uploaded_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-ac6a19ded7"></a>`last_verified_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-652279a98a"></a>`object_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-1bcd53b509"></a>`state` | yes | type="string"; const="failed" |  |
| <a id="s-6ba0d92e03"></a>`storage_prefix` | yes | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-d030c4944a"></a>`store` | yes | #/components/schemas/ArchiveStoreName |  |
| <a id="s-c582d68251"></a>`stored_bytes` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: FailedArchiveRootPublicationOut](schemas-failedarchiverootpublicationout.md)

## Governing policies

- <a id="pa-e1f5efa7f8"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/FailedArchiveCopyOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a6f0d8671c86982c2022d1fa4ac56abd480f73282ef86af421e4576ad589c74 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_root": {
      "$ref": "#/components/schemas/FailedArchiveRootPublicationOut"
    },
    "failure": {
      "minLength": 1,
      "title": "Failure",
      "type": "string"
    },
    "last_uploaded_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Last Uploaded At"
    },
    "last_verified_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Last Verified At"
    },
    "object_count": {
      "minimum": 0,
      "title": "Object Count",
      "type": "integer"
    },
    "state": {
      "const": "failed",
      "title": "State",
      "type": "string"
    },
    "storage_prefix": {
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Storage Prefix"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "stored_bytes": {
      "minimum": 0,
      "title": "Stored Bytes",
      "type": "integer"
    }
  },
  "required": [
    "store",
    "storage_prefix",
    "object_count",
    "stored_bytes",
    "last_uploaded_at",
    "last_verified_at",
    "archive_root",
    "state",
    "failure"
  ],
  "title": "FailedArchiveCopyOut",
  "type": "object"
}
```
