# schemas: OmittedCollectionFileProvenancePage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-omittedcollectionfileprovenancepage:0d4bda6c12 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-7f7d0e9982"></a>
- <a id="s-c53f04453f"></a>`title`: OmittedCollectionFileProvenancePage
- <a id="s-3da2c7c5d8"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cfc0052d4d"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-0f29aaf51f"></a>`files` | yes | type="array"; items=(#/components/schemas/OmittedCollectionFileProvenanceOut) |  |
| <a id="s-815d243b32"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-9394ddb730"></a>`order` | yes | #/components/schemas/SortOrder |  |
| <a id="s-13afc5c593"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-9f7c693e4f"></a>`provenance_identity` | yes | type="null" |  |
| <a id="s-e2a2173982"></a>`provenance_mode` | yes | type="string"; const="omitted" |  |
| <a id="s-62388817d0"></a>`query` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-6546da24d2"></a>`sort` | yes | #/components/schemas/ProvenanceSort |  |
| <a id="s-b4216cdce6"></a>`status` | yes | anyOf=#/components/schemas/ProvenanceStatus \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-0f29aaf51f) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-13afc5c593) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: OmittedCollectionFileProvenanceOut](schemas-omittedcollectionfileprovenanceout.md)
- [schemas: ProvenanceSort](schemas-provenancesort.md)
- [schemas: ProvenanceStatus](schemas-provenancestatus.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-b07c0c43cd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-c63d65e3f8"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-084d6c1a6e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OmittedCollectionFileProvenancePage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4e0bcbee438bf78c2cb28fd0b8815533930a2c131df3393a0c28b175d839fcf4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/OmittedCollectionFileProvenanceOut"
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
      "title": "Provenance Identity",
      "type": "null"
    },
    "provenance_mode": {
      "const": "omitted",
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
  "title": "OmittedCollectionFileProvenancePage",
  "type": "object"
}
```
