# schemas: KeyDownloadQuotaListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-keydownloadquotalistout:ee3a9d1724 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: KeyDownloadQuotaListOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `active` | yes | anyOf=type="boolean" \| type="null" |  |
| `app` | yes | anyOf=#/components/schemas/ApplicationName \| type="null" |  |
| `next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| `query` | yes | anyOf=type="string" \| type="null" |  |
| `quotas` | yes | type="array"; items=(#/components/schemas/KeyDownloadQuotaOut) |  |
| `sort` | yes | #/components/schemas/DownloadQuotaSort |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: DownloadQuotaSort](schemas-downloadquotasort.md)
- [schemas: KeyDownloadQuotaOut](schemas-keydownloadquotaout.md)
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

- `/external_contract/http_openapi/riverhog/components/schemas/KeyDownloadQuotaListOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d68828b6fa1bc770b61fd004382ac4add988c481d8466b71720edb67bae1e564 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "active": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "title": "Active"
    },
    "app": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ApplicationName"
        },
        {
          "type": "null"
        }
      ]
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
    "quotas": {
      "items": {
        "$ref": "#/components/schemas/KeyDownloadQuotaOut"
      },
      "title": "Quotas",
      "type": "array"
    },
    "sort": {
      "$ref": "#/components/schemas/DownloadQuotaSort"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "app",
    "active",
    "quotas"
  ],
  "title": "KeyDownloadQuotaListOut",
  "type": "object"
}
```
