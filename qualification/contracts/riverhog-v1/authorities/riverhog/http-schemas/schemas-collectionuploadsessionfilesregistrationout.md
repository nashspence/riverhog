# schemas: CollectionUploadSessionFilesRegistrationOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadsessionfilesregistrationout:c6165b4837 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-811bf04556"></a>

- <a id="s-8dcfe4dc34"></a>`type`: `"object"`
- <a id="s-05a29b93d8"></a>`additionalProperties`: `false`
- <a id="s-3eb13a66ff"></a>`required`: `["collection_id","ingest_source","archive_store","encryption_format","passphrase_id","state","files","volumes"]`
- <a id="s-cee17c133a"></a>`title`: `"CollectionUploadSessionFilesRegistrationOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d57f9a95ca"></a>`archive_store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-706f44817b"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-ea443b1f02"></a>`encryption_format` | yes | type="string"; title="Encryption Format" |  |
| <a id="s-c35c83ee48"></a>`files` | yes | type="array"; items=([CollectionUploadFileOut](schemas-collectionuploadfileout.md)); title="Files" |  |
| <a id="s-d8b867d200"></a>`ingest_source` | yes | anyOf=[(type="string"); (type="null")]; title="Ingest Source" |  |
| <a id="s-a22c58005a"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$"; title="Passphrase Id" |  |
| <a id="s-c9c25cce1c"></a>`state` | yes | type="string"; const="open"; title="State" |  |
| <a id="s-c488bbdba5"></a>`volumes` | yes | type="array"; items=([CollectionUploadVolumeSummaryDocument](schemas-collectionuploadvolumesummarydocument.md)); title="Volumes" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-c35c83ee48) | `cardinality · items · operational_policy` | shared above |
| [field volumes](#s-c488bbdba5) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)
- [CollectionUploadFileOut](schemas-collectionuploadfileout.md)
- [CollectionUploadVolumeSummaryDocument](schemas-collectionuploadvolumesummarydocument.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2fcb0f669b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-3bff10283e"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadSessionFilesRegistrationOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
