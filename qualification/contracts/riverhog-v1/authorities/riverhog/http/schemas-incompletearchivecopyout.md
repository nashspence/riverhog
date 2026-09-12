# schemas: IncompleteArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-incompletearchivecopyout:08296c964e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f610daf7ae"></a>
- <a id="s-510f8e5de4"></a>`title`: IncompleteArchiveCopyOut
- <a id="s-b5c898e071"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-465b66cfb2"></a>`archive_root` | yes | anyOf=#/components/schemas/PendingArchiveRootPublicationOut \| #/components/schemas/UploadedArchiveRootPublicationOut |  |
| <a id="s-461f9bfdc3"></a>`failure` | yes | type="null" |  |
| <a id="s-91c7e1fec3"></a>`last_uploaded_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-21e4a90300"></a>`last_verified_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-c956ecabd4"></a>`object_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-126da85264"></a>`state` | yes | type="string"; enum=["pending","uploading","retrying"] |  |
| <a id="s-2c41fde9f8"></a>`storage_prefix` | yes | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-624ee4c956"></a>`store` | yes | #/components/schemas/ArchiveStoreName |  |
| <a id="s-2e70f18438"></a>`stored_bytes` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: PendingArchiveRootPublicationOut](schemas-pendingarchiverootpublicationout.md)
- [schemas: UploadedArchiveRootPublicationOut](schemas-uploadedarchiverootpublicationout.md)

## Governing policies

- <a id="pa-16f5b2ed46"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/IncompleteArchiveCopyOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 03f626031ce0ea3a25b02a5259193a1083c2047db6f52e8e72ae31d6c7c24e5a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_root": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/PendingArchiveRootPublicationOut"
        },
        {
          "$ref": "#/components/schemas/UploadedArchiveRootPublicationOut"
        }
      ],
      "title": "Archive Root"
    },
    "failure": {
      "title": "Failure",
      "type": "null"
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
      "enum": [
        "pending",
        "uploading",
        "retrying"
      ],
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
  "title": "IncompleteArchiveCopyOut",
  "type": "object"
}
```
