# schemas: CollectionTagListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiontaglistout:790bfb4209 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTagListOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CollectionTag](schemas-collectiontag.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |

## Contract summary

- `title`: CollectionTagListOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `next_page_token` | yes | object (1 fields) |  |
| `page_size` | yes | integer |  |
| `revision` | yes | integer |  |
| `tag_set_identity` | yes | string |  |
| `tags` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9842f4b5487bf8be7af46821a59efdf08f93bd5ad6368a3470be8094cac56011 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
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
    "page_size": {
      "maximum": 100,
      "minimum": 1,
      "title": "Page Size",
      "type": "integer"
    },
    "revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    },
    "tag_set_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Tag Set Identity",
      "type": "string"
    },
    "tags": {
      "items": {
        "$ref": "#/components/schemas/CollectionTag"
      },
      "title": "Tags",
      "type": "array"
    }
  },
  "required": [
    "collection_id",
    "revision",
    "tag_set_identity",
    "page_size",
    "next_page_token",
    "tags"
  ],
  "title": "CollectionTagListOut",
  "type": "object"
}
```
