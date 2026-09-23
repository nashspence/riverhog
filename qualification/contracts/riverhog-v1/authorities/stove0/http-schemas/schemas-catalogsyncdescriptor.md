# schemas: CatalogSyncDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-catalogsyncdescriptor:30b0d5d025 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f02e615f06"></a>

- <a id="s-934d07f6c5"></a>`type`: `"object"`
- <a id="s-984f32413c"></a>`additionalProperties`: `false`
- <a id="s-1f5a23634e"></a>`required`: `["collection_id","archive_root_sha256","content_identity","description","description_revision","description_identity","tag_revision","tag_set_identity","revision"]`
- <a id="s-105c0bd5a8"></a>`title`: `"CatalogSyncDescriptor"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8ad78fe25f"></a>`archive_root_sha256` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-6d82f6e566"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-6c31e8327f"></a>`content_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |
| <a id="s-08da37b2cc"></a>`description` | yes | anyOf=[([CollectionDescription](schemas-collectiondescription.md)); (type="null")] |  |
| <a id="s-e9cfc0106a"></a>`description_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$"; title="Description Identity" |  |
| <a id="s-2d3eed1f81"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991; title="Description Revision" |  |
| <a id="s-0cca83b726"></a>`revision` | yes | type="string"; maxLength=19; minLength=1; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$"; title="Revision" |  |
| <a id="s-f0e881c2df"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991; title="Tag Revision" |  |
| <a id="s-fb420e0220"></a>`tag_set_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$"; title="Tag Set Identity" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_root_sha256](#s-8ad78fe25f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field content_identity](#s-6c31e8327f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field description_identity](#s-e9cfc0106a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field description_revision](#s-2d3eed1f81) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=0; reason="schema-maximum" |
| [field revision](#s-0cca83b726) | `length · characters · contract_max` | maximum=19; minimum=1; reason="schema-maximum" |
| [field tag_revision](#s-f0e881c2df) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |
| [field tag_set_identity](#s-fb420e0220) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |

## Maintained corroboration

### Referenced contract elements

- [CollectionDescription](schemas-collectiondescription.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-940fc0968d"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-9be704e53c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CatalogSyncDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0b2fef3a565b2c8252cf0fb222015209209a4f9cc7479cae0304afe6d46d061 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "archive_root_sha256": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Archive Root Sha256",
      "type": "string"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "content_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Content Identity",
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
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Description Identity",
      "type": "string"
    },
    "description_revision": {
      "maximum": 9007199254740991,
      "minimum": 0,
      "title": "Description Revision",
      "type": "integer"
    },
    "revision": {
      "maxLength": 19,
      "minLength": 1,
      "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
      "title": "Revision",
      "type": "string"
    },
    "tag_revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "title": "Tag Revision",
      "type": "integer"
    },
    "tag_set_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Tag Set Identity",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "archive_root_sha256",
    "content_identity",
    "description",
    "description_revision",
    "description_identity",
    "tag_revision",
    "tag_set_identity",
    "revision"
  ],
  "title": "CatalogSyncDescriptor",
  "type": "object"
}
```

</details>
