# schemas: ArchiveCopyJobListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyjoblistout:4d316d5212 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: ArchiveCopyJobListOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `copies` | yes | type="array"; items=(#/components/schemas/ArchiveCopyJobOut) |  |
| `filters` | yes | #/components/schemas/ArchiveCopyJobListFiltersOut |  |
| `next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| `query` | yes | anyOf=type="string" \| type="null" |  |
| `sort` | yes | #/components/schemas/ArchiveCopySort |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveCopyJobListFiltersOut](schemas-archivecopyjoblistfiltersout.md)
- [schemas: ArchiveCopyJobOut](schemas-archivecopyjobout.md)
- [schemas: ArchiveCopySort](schemas-archivecopysort.md)
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

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobListOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30eab820038f814255f58c7bc6e17b93f72f43e4fd06287596eac6dea9ef82c1 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "copies": {
      "items": {
        "$ref": "#/components/schemas/ArchiveCopyJobOut"
      },
      "title": "Copies",
      "type": "array"
    },
    "filters": {
      "$ref": "#/components/schemas/ArchiveCopyJobListFiltersOut"
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
      "$ref": "#/components/schemas/ArchiveCopySort"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "filters",
    "copies"
  ],
  "title": "ArchiveCopyJobListOut",
  "type": "object"
}
```
