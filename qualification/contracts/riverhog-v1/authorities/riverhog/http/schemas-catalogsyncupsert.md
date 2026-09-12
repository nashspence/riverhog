# schemas: CatalogSyncUpsert

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-catalogsyncupsert:14fa1be7c8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

<a id="s-8711c355e2"></a>
- <a id="s-6644b70f71"></a>`title`: CatalogSyncUpsert
- <a id="s-f7ad15979d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c8a4a01aa1"></a>`archive_root_sha256` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a2b1ded4cb"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-5f2c06192f"></a>`content_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-effc539626"></a>`description` | yes | anyOf=#/components/schemas/CollectionDescription \| type="null" |  |
| <a id="s-3d8c8564c3"></a>`description_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-37af96b838"></a>`description_revision` | yes | type="integer"; minimum=0; maximum=9007199254740991 |  |
| <a id="s-fe82889f13"></a>`operation` | no | type="string"; const="upsert" |  |
| <a id="s-beb58546cc"></a>`revision` | yes | type="string"; minLength=1; maxLength=19; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |
| <a id="s-de502899cb"></a>`tag_revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-9974c886dd"></a>`tag_set_identity` | yes | type="string"; minLength=64; maxLength=64; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_root_sha256](#s-c8a4a01aa1) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field content_identity](#s-5f2c06192f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field description_identity](#s-3d8c8564c3) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field description_revision](#s-37af96b838) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=0; reason="schema-maximum" |
| [field revision](#s-beb58546cc) | `length · characters · contract_max` | maximum=19; minimum=1; reason="schema-maximum" |
| [field tag_revision](#s-de502899cb) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |
| [field tag_set_identity](#s-9974c886dd) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionDescription](schemas-collectiondescription.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-d4523680d4"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-ea5877eeb0"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncUpsert`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14087b9a31e6f96931a1a7b0700aa05869c0c66b85f467adfbabd52eeccfb29e -->

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
    "operation": {
      "const": "upsert",
      "default": "upsert",
      "title": "Operation",
      "type": "string"
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
  "title": "CatalogSyncUpsert",
  "type": "object"
}
```
