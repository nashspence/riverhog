# schemas: ListCollectionsResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-listcollectionsresponse:cbb9817340 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-d9858fd321ec"></a>
- <a id="s-54809b9c434a"></a>`title`: ListCollectionsResponse
- <a id="s-77f4f9b8da21"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1d875f0d2603"></a>`collections` | yes | type="array"; items=(#/components/schemas/CollectionSummaryOut) |  |
| <a id="s-a2ba0da47981"></a>`encryption_format` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-a18815df9e5d"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-e0b450d7ecfb"></a>`order` | yes | #/components/schemas/SortOrder |  |
| <a id="s-f6b9eb37e966"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-585f47498ae0"></a>`passphrase_id` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-316d14721e63"></a>`query` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-15d2c2d92482"></a>`sort` | yes | #/components/schemas/CollectionSort |  |
| <a id="s-3ce334b3ba99"></a>`tags` | yes | type="array"; maxItems=100; items=(#/components/schemas/CollectionTag); additional keys=`x-riverhog-extent` |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collections](#s-1d875f0d2603) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=100

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-f6b9eb37e966) | `value · schema-value · contract_max` | minimum=1; reason="schema-maximum" |
| [field tags](#s-3ce334b3ba99) | `cardinality · items · contract_max` | reason="bounded-exact-tag-selector-batch" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CollectionSort](schemas-collectionsort.md)
- [schemas: CollectionSummaryOut](schemas-collectionsummaryout.md)
- [schemas: CollectionTag](schemas-collectiontag.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-97cae48eab38"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-aad9f6e7edf3"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-268551f70b95"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListCollectionsResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea51bf4002f224d45c514963b53be98ac4990d8f31436df87fabbb2fd3765c9d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collections": {
      "items": {
        "$ref": "#/components/schemas/CollectionSummaryOut"
      },
      "title": "Collections",
      "type": "array"
    },
    "encryption_format": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Encryption Format"
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
    "passphrase_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Passphrase Id"
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
      "$ref": "#/components/schemas/CollectionSort"
    },
    "tags": {
      "items": {
        "$ref": "#/components/schemas/CollectionTag"
      },
      "maxItems": 100,
      "title": "Tags",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-exact-tag-selector-batch"
      }
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "encryption_format",
    "passphrase_id",
    "tags",
    "collections"
  ],
  "title": "ListCollectionsResponse",
  "type": "object"
}
```
