# schemas: UploadedArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-uploadedarchivecopyout:ec61f796c8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-1ee1a50656"></a>

- <a id="s-f58244b2fc"></a>`type`: `"object"`
- <a id="s-63ab2e4253"></a>`additionalProperties`: `false`
- <a id="s-27eff04cc4"></a>`required`: `["store","storage_prefix","object_count","stored_bytes","last_uploaded_at","last_verified_at","archive_root","state","failure"]`
- <a id="s-e0c41366e6"></a>`title`: `"UploadedArchiveCopyOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b74b77804b"></a>`archive_root` | yes | [UploadedArchiveRootPublicationOut](schemas-uploadedarchiverootpublicationout.md) |  |
| <a id="s-b5bad9ada5"></a>`failure` | yes | type="null"; title="Failure" |  |
| <a id="s-13dd7ad5e1"></a>`last_uploaded_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Last Uploaded At" |  |
| <a id="s-f8bff805af"></a>`last_verified_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Last Verified At" |  |
| <a id="s-371b648909"></a>`object_count` | yes | type="integer"; minimum=1; title="Object Count" |  |
| <a id="s-f991e3fee3"></a>`state` | yes | type="string"; const="uploaded"; title="State" |  |
| <a id="s-646f4c3a71"></a>`storage_prefix` | yes | type="string"; minLength=1; title="Storage Prefix" |  |
| <a id="s-b2892d99d7"></a>`store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |
| <a id="s-dac2d5a267"></a>`stored_bytes` | yes | type="integer"; minimum=1; title="Stored Bytes" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field last_uploaded_at](#s-13dd7ad5e1) | `length · characters · fixed` | shared above |
| [field last_verified_at](#s-f8bff805af) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ArchiveStoreName](schemas-archivestorename.md)
- [UploadedArchiveRootPublicationOut](schemas-uploadedarchiverootpublicationout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8d34102f41"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-fecd3a0701"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/UploadedArchiveCopyOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 81ecf14fbd03595adbdf6f984f5f4150cc0e25a600f362de4c7bcc658584c57a -->

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
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Last Uploaded At",
      "type": "string"
    },
    "last_verified_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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

</details>
