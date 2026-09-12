# schemas: ArchiveStoreListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivestorelistout:2033e2cdf5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: ArchiveStoreListOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| `query` | yes | anyOf=type="string" \| type="null" |  |
| `sort` | yes | #/components/schemas/ArchiveStoreSort |  |
| `stores` | yes | type="array"; items=(#/components/schemas/ArchiveStoreOut) |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreOut](schemas-archivestoreout.md)
- [schemas: ArchiveStoreSort](schemas-archivestoresort.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveStoreListOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ebf6709f30caf2de00115ffe77c6122fd87116c2cbc17af37bcbdaa0100723a -->

```json
{
  "additionalProperties": false,
  "properties": {
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
      "$ref": "#/components/schemas/ArchiveStoreSort"
    },
    "stores": {
      "items": {
        "$ref": "#/components/schemas/ArchiveStoreOut"
      },
      "title": "Stores",
      "type": "array"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "stores"
  ],
  "title": "ArchiveStoreListOut",
  "type": "object"
}
```
