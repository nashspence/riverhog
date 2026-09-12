# schemas: OmittedCollectionFileProvenancePage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-omittedcollectionfileprovenancepage:0d4bda6c12 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/OmittedCollectionFileProvenancePage`

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
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: OmittedCollectionFileProvenanceOut](schemas-omittedcollectionfileprovenanceout.md)
- [schemas: ProvenanceSort](schemas-provenancesort.md)
- [schemas: ProvenanceStatus](schemas-provenancestatus.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: OmittedCollectionFileProvenancePage
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `files` | yes | array |  |
| `next_page_token` | yes | object (1 fields) |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | integer |  |
| `provenance_identity` | yes | null |  |
| `provenance_mode` | yes | string |  |
| `query` | yes | object (2 fields) |  |
| `sort` | yes | #/components/schemas/ProvenanceSort |  |
| `status` | yes | object (1 fields) |  |

## Complete owned contract

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
