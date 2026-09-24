# schemas: IncompleteArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-incompletearchivecopyout:48b00424af -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f610daf7ae"></a>

- <a id="s-b5c898e071"></a>`type`: `"object"`
- <a id="s-6fa117b206"></a>`additionalProperties`: `false`
- <a id="s-b98a550363"></a>`required`: `["store","storage_prefix","object_count","stored_bytes","last_uploaded_at","last_verified_at","archive_root","state","failure"]`
- <a id="s-510f8e5de4"></a>`title`: `"IncompleteArchiveCopyOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-465b66cfb2"></a>`archive_root` | yes | anyOf=[([PendingArchiveRootPublicationOut](schemas-pendingarchiverootpublicationout.md)); ([UploadedArchiveRootPublicationOut](schemas-uploadedarchiverootpublicationout.md))]; title="Archive Root" |  |
| <a id="s-461f9bfdc3"></a>`failure` | yes | type="null"; title="Failure" |  |
| <a id="s-91c7e1fec3"></a>`last_uploaded_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Last Uploaded At" |  |
| <a id="s-21e4a90300"></a>`last_verified_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Last Verified At" |  |
| <a id="s-c956ecabd4"></a>`object_count` | yes | type="integer"; minimum=0; title="Object Count" |  |
| <a id="s-126da85264"></a>`state` | yes | type="string"; enum=["pending","uploading","retrying"]; title="State" |  |
| <a id="s-2c41fde9f8"></a>`storage_prefix` | yes | anyOf=[(type="string"; minLength=1); (type="null")]; title="Storage Prefix" |  |
| <a id="s-624ee4c956"></a>`store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-2e70f18438"></a>`stored_bytes` | yes | type="integer"; minimum=0; title="Stored Bytes" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-fd3c7c37de"></a>[field last_uploaded_at · string value](#s-91c7e1fec3) | `length · characters · fixed` | shared above |
| <a id="s-8434123607"></a>[field last_verified_at · string value](#s-21e4a90300) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)
- [PendingArchiveRootPublicationOut](schemas-pendingarchiverootpublicationout.md)
- [UploadedArchiveRootPublicationOut](schemas-uploadedarchiverootpublicationout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-4f3c6c6e2b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-585da3f41a"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/IncompleteArchiveCopyOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 770f6b1e2330e1bf816942ae36e306e870d160dfdee8f68d2c358741f8776185 -->

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
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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

</details>
