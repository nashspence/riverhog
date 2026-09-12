# schemas: CatalogSyncDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-catalogsyncdescriptor:841efe3b7b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

<a id="s-8e6a59f082"></a>
- <a id="s-472ac7ad4b"></a>`title`: CatalogSyncDescriptor
- <a id="s-f11fd59494"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ff143d900d"></a>`archive_root_sha256` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-75e947c024"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-eda7115168"></a>`content_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0acf4a3f6c"></a>`description` | yes | anyOf=#/components/schemas/CollectionDescription \| type="null" |  |
| <a id="s-c2e2616dd7"></a>`description_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6e54f11a9e"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-d06f1868a6"></a>`revision` | yes | type="string"; minLength=1; maxLength=19; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-e2cc0388c1"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-f40cefb16d"></a>`tag_set_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_root_sha256](#s-ff143d900d) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field content_identity](#s-eda7115168) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field description_identity](#s-c2e2616dd7) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field description_revision](#s-6e54f11a9e) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=0; reason="schema-maximum" |
| [field revision](#s-d06f1868a6) | `length · characters · contract_max` | maximum=19; minimum=1; reason="schema-maximum" |
| [field tag_revision](#s-e2cc0388c1) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |
| [field tag_set_identity](#s-f40cefb16d) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionDescription](schemas-collectiondescription.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-c0a1047528"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-25bf47a6ca"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncDescriptor`

### Exact owned JSON

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
