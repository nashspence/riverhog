# schemas: UploadedArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-uploadedarchivecopyout:e859b617b5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1ee1a5065635"></a>
- <a id="s-e0c41366e6d9"></a>`title`: UploadedArchiveCopyOut
- <a id="s-f58244b2fc5e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b74b77804b78"></a>`archive_root` | yes | #/components/schemas/UploadedArchiveRootPublicationOut |  |
| <a id="s-b5bad9ada57b"></a>`failure` | yes | type="null" |  |
| <a id="s-13dd7ad5e1e9"></a>`last_uploaded_at` | yes | type="string" |  |
| <a id="s-f8bff805afe5"></a>`last_verified_at` | yes | type="string" |  |
| <a id="s-371b6489099e"></a>`object_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-f991e3fee3da"></a>`state` | yes | type="string"; const="uploaded" |  |
| <a id="s-646f4c3a71a4"></a>`storage_prefix` | yes | type="string"; minLength=1 |  |
| <a id="s-b2892d99d774"></a>`store` | yes | #/components/schemas/ArchiveStoreName |  |
| <a id="s-dac2d5a2672b"></a>`stored_bytes` | yes | type="integer"; minimum=1 |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: UploadedArchiveRootPublicationOut](schemas-uploadedarchiverootpublicationout.md)

## Governing policies

- <a id="pa-35cf683914b6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/UploadedArchiveCopyOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 92b812376a7370b1f76bf65d5ed9af571721497a1ad4dd6df0c3fee0eb5968f0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_root": {
      "$ref": "#/components/schemas/UploadedArchiveRootPublicationOut"
    },
    "failure": {
      "title": "Failure",
      "type": "null"
    },
    "last_uploaded_at": {
      "title": "Last Uploaded At",
      "type": "string"
    },
    "last_verified_at": {
      "title": "Last Verified At",
      "type": "string"
    },
    "object_count": {
      "minimum": 1,
      "title": "Object Count",
      "type": "integer"
    },
    "state": {
      "const": "uploaded",
      "title": "State",
      "type": "string"
    },
    "storage_prefix": {
      "minLength": 1,
      "title": "Storage Prefix",
      "type": "string"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "stored_bytes": {
      "minimum": 1,
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
  "title": "UploadedArchiveCopyOut",
  "type": "object"
}
```
