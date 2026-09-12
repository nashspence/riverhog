# schemas: ListCollectionsResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-listcollectionsresponse:cbb9817340 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListCollectionsResponse`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CollectionSort](schemas-collectionsort.md)
- [schemas: CollectionSummaryOut](schemas-collectionsummaryout.md)
- [schemas: CollectionTag](schemas-collectiontag.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| cardinality | items | `contract_max` | maximum=100, reason=bounded-exact-tag-selector-batch |

## Contract summary

- `title`: ListCollectionsResponse
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collections` | yes | array |  |
| `encryption_format` | yes | object (2 fields) |  |
| `next_page_token` | yes | object (1 fields) |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | integer |  |
| `passphrase_id` | yes | object (2 fields) |  |
| `query` | yes | object (2 fields) |  |
| `sort` | yes | #/components/schemas/CollectionSort |  |
| `tags` | yes | array |  |

## Complete owned contract

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
