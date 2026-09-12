# schemas: MixedCollectionFileProvenancePage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-mixedcollectionfileprovenancepage:47fef0b483 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-f293d0881c"></a>
- <a id="s-e5f0a69c1a"></a>`title`: MixedCollectionFileProvenancePage
- <a id="s-b9bf592ddb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-062f950653"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-a8aaa69081"></a>`files` | yes | type="array"; items=(#/components/schemas/_FileProvenanceOut) |  |
| <a id="s-e9387bae1e"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-3d7995e4f3"></a>`order` | yes | #/components/schemas/SortOrder |  |
| <a id="s-4aa8fcb191"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-5407617a49"></a>`provenance_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c6fe5e265a"></a>`provenance_mode` | yes | type="string"; const="mixed" |  |
| <a id="s-a43bcf27e1"></a>`query` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-2832543499"></a>`sort` | yes | #/components/schemas/ProvenanceSort |  |
| <a id="s-f944118fc7"></a>`status` | yes | anyOf=#/components/schemas/ProvenanceStatus \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-a8aaa69081) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-4aa8fcb191) | `value · schema-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum" |
| [field provenance_identity](#s-5407617a49) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: ProvenanceSort](schemas-provenancesort.md)
- [schemas: ProvenanceStatus](schemas-provenancestatus.md)
- [schemas: SortOrder](schemas-sortorder.md)
- [schemas: _FileProvenanceOut](schemas-fileprovenanceout.md)

## Governing policies

- <a id="pa-ee791b6147"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-9b96e6f745"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-39785fd431"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/MixedCollectionFileProvenancePage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ccef053199f8dc3e65eefb86d70d3f3baa52416018128a30096ed1329eefc36 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/_FileProvenanceOut"
      },
      "title": "Files",
      "type": "array"
    },
    "next_page_token": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/BrowsePageToken"
        },
        {
          "type": "null"
        }
      ]
    },
    "order": {
      "$ref": "#/components/schemas/SortOrder"
    },
    "page_size": {
      "maximum": 100,
      "minimum": 1,
      "title": "Page Size",
      "type": "integer"
    },
    "provenance_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Provenance Identity",
      "type": "string"
    },
    "provenance_mode": {
      "const": "mixed",
      "title": "Provenance Mode",
      "type": "string"
    },
    "query": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Query"
    },
    "sort": {
      "$ref": "#/components/schemas/ProvenanceSort"
    },
    "status": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ProvenanceStatus"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "status",
    "collection_id",
    "provenance_mode",
    "provenance_identity",
    "files"
  ],
  "title": "MixedCollectionFileProvenancePage",
  "type": "object"
}
```
