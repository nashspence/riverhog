# schemas: KeyDownloadQuotaListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-keydownloadquotalistout:ee3a9d1724 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/KeyDownloadQuotaListOut`

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

- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: DownloadQuotaSort](schemas-downloadquotasort.md)
- [schemas: KeyDownloadQuotaOut](schemas-keydownloadquotaout.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |

## Contract summary

- `title`: KeyDownloadQuotaListOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `active` | yes | object (2 fields) |  |
| `app` | yes | object (1 fields) |  |
| `next_page_token` | yes | object (1 fields) |  |
| `order` | yes | #/components/schemas/SortOrder |  |
| `page_size` | yes | integer |  |
| `query` | yes | object (2 fields) |  |
| `quotas` | yes | array |  |
| `sort` | yes | #/components/schemas/DownloadQuotaSort |  |

## Complete owned contract

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
