# schemas: CollectionUploadSessionFilesRegistrationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadsessionfilesregistrationout:ebe60d1b35 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-811bf0455639"></a>
- <a id="s-cee17c133adc"></a>`title`: CollectionUploadSessionFilesRegistrationOut
- <a id="s-8dcfe4dc347a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d57f9a95cadf"></a>`archive_store` | yes | #/components/schemas/ArchiveStoreName |  |
| <a id="s-706f44817b8e"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-ea443b1f02b5"></a>`encryption_format` | yes | type="string" |  |
| <a id="s-c35c83ee48a2"></a>`files` | yes | type="array"; items=(#/components/schemas/CollectionUploadFileOut) |  |
| <a id="s-d8b867d20040"></a>`ingest_source` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-a22c58005ac0"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$" |  |
| <a id="s-c9c25cce1c2a"></a>`state` | yes | type="string"; const="open" |  |
| <a id="s-c488bbdba50d"></a>`volumes` | yes | type="array"; items=(#/components/schemas/CollectionUploadVolumeSummaryDocument) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-c35c83ee48a2) | `cardinality · items · operational_policy` | shared above |
| [field volumes](#s-c488bbdba50d) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CollectionUploadFileOut](schemas-collectionuploadfileout.md)
- [schemas: CollectionUploadVolumeSummaryDocument](schemas-collectionuploadvolumesummarydocument.md)

## Governing policies

- <a id="pa-53f204c911a3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-db9ef1239aed"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadSessionFilesRegistrationOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ea268b0d0e5b2ad156f8df332d294470f81bb8928a6ad38a70ea7ba067f2f07 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "encryption_format": {
      "title": "Encryption Format",
      "type": "string"
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/CollectionUploadFileOut"
      },
      "title": "Files",
      "type": "array"
    },
    "ingest_source": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Ingest Source"
    },
    "passphrase_id": {
      "pattern": "^[A-Za-z0-9_-]{16,128}$",
      "title": "Passphrase Id",
      "type": "string"
    },
    "state": {
      "const": "open",
      "title": "State",
      "type": "string"
    },
    "volumes": {
      "items": {
        "$ref": "#/components/schemas/CollectionUploadVolumeSummaryDocument"
      },
      "title": "Volumes",
      "type": "array"
    }
  },
  "required": [
    "collection_id",
    "ingest_source",
    "archive_store",
    "encryption_format",
    "passphrase_id",
    "state",
    "files",
    "volumes"
  ],
  "title": "CollectionUploadSessionFilesRegistrationOut",
  "type": "object"
}
```
