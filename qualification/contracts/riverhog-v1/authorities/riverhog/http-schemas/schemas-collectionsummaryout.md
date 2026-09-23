# schemas: CollectionSummaryOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionsummaryout:bc4300288e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-ec21417e93"></a>

- <a id="s-0497c24fb4"></a>`type`: `"object"`
- <a id="s-a88d3dc252"></a>`additionalProperties`: `false`
- <a id="s-1eae68b762"></a>`required`: `["id","created_at","description","description_revision","description_identity","description_publication","tag_revision","tag_set_identity","tag_publication","content_identity","archive_root_sha256","encryption_format","passphrase_id","files","bytes","remote_storage_bytes","archive_copy_count"]`
- <a id="s-9c10b29a9c"></a>`title`: `"CollectionSummaryOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a912d584fe"></a>`archive_copy_count` | yes | type="integer"; minimum=0; title="Archive Copy Count" |  |
| <a id="s-da1c3dedb5"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-96e1750701"></a>`bytes` | yes | type="integer"; title="Bytes" |  |
| <a id="s-4a2c71fa69"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |
| <a id="s-f767b31dab"></a>`created_at` | yes | type="string"; title="Created At" |  |
| <a id="s-4233142e27"></a>`description` | yes | anyOf=[([CollectionDescription](schemas-collectiondescription.md)); (type="null")] |  |
| <a id="s-f7df86a094"></a>`description_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Description Identity" |  |
| <a id="s-453cf07a7d"></a>`description_publication` | yes | type="string"; enum=["not_required","current","reconciling"]; title="Description Publication" |  |
| <a id="s-f993f04535"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991; title="Description Revision" |  |
| <a id="s-9ef8c02bd0"></a>`encryption_format` | yes | type="string"; title="Encryption Format" |  |
| <a id="s-7dac470243"></a>`files` | yes | type="integer"; title="Files" |  |
| <a id="s-06047289c9"></a>`id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-0515826c3d"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$"; title="Passphrase Id" |  |
| <a id="s-22d068ac76"></a>`remote_storage_bytes` | yes | type="integer"; title="Remote Storage Bytes" |  |
| <a id="s-d0489daf39"></a>`tag_publication` | yes | type="string"; enum=["current","reconciling"]; title="Tag Publication" |  |
| <a id="s-a4c8cd0f70"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991; title="Tag Revision" |  |
| <a id="s-15a8f883f7"></a>`tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Tag Set Identity" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-96e1750701) | `value · schema-value · operational_policy` | shared above |
| [field files](#s-7dac470243) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_root_sha256](#s-da1c3dedb5) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field content_identity](#s-4a2c71fa69) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field description_identity](#s-f7df86a094) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field description_revision](#s-f993f04535) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=0; reason="schema-maximum" |
| [field tag_revision](#s-a4c8cd0f70) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |
| [field tag_set_identity](#s-15a8f883f7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [CollectionDescription](schemas-collectiondescription.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8ac3c81867"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-990a2ec446"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-b6cb269e15"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionSummaryOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4cd5a125d37eac748c8a3bb9e6b711adaefc92edcc8b19d4485cdff4b505e975 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_copy_count": {
      "minimum": 0,
      "title": "Archive Copy Count",
      "type": "integer"
    },
    "archive_root_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Archive Root Sha256",
      "type": "string"
    },
    "bytes": {
      "title": "Bytes",
      "type": "integer"
    },
    "content_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Content Identity",
      "type": "string"
    },
    "created_at": {
      "title": "Created At",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionDescription"
        },
        {
          "type": "null"
        }
      ]
    },
    "description_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Description Identity",
      "type": "string"
    },
    "description_publication": {
      "enum": [
        "not_required",
        "current",
        "reconciling"
      ],
      "title": "Description Publication",
      "type": "string"
    },
    "description_revision": {
      "maximum": 9007199254740991,
      "minimum": 0,
      "title": "Description Revision",
      "type": "integer"
    },
    "encryption_format": {
      "title": "Encryption Format",
      "type": "string"
    },
    "files": {
      "title": "Files",
      "type": "integer"
    },
    "id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "passphrase_id": {
      "pattern": "^[A-Za-z0-9_-]{16,128}$",
      "title": "Passphrase Id",
      "type": "string"
    },
    "remote_storage_bytes": {
      "title": "Remote Storage Bytes",
      "type": "integer"
    },
    "tag_publication": {
      "enum": [
        "current",
        "reconciling"
      ],
      "title": "Tag Publication",
      "type": "string"
    },
    "tag_revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "title": "Tag Revision",
      "type": "integer"
    },
    "tag_set_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Tag Set Identity",
      "type": "string"
    }
  },
  "required": [
    "id",
    "created_at",
    "description",
    "description_revision",
    "description_identity",
    "description_publication",
    "tag_revision",
    "tag_set_identity",
    "tag_publication",
    "content_identity",
    "archive_root_sha256",
    "encryption_format",
    "passphrase_id",
    "files",
    "bytes",
    "remote_storage_bytes",
    "archive_copy_count"
  ],
  "title": "CollectionSummaryOut",
  "type": "object"
}
```

</details>
