# schemas: KeyDownloadQuotaListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-keydownloadquotalistout:ee3a9d1724 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-dd95c26113fb"></a>
- <a id="s-25f631f39fe7"></a>`title`: KeyDownloadQuotaListOut
- <a id="s-3d13e0435937"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2bd88bec1e81"></a>`active` | yes | anyOf=type="boolean" \| type="null" |  |
| <a id="s-61d180626b98"></a>`app` | yes | anyOf=#/components/schemas/ApplicationName \| type="null" |  |
| <a id="s-185405e83df0"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-fafda38a6947"></a>`order` | yes | #/components/schemas/SortOrder |  |
| <a id="s-ea9fc70dcabd"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-4cccd8c9cf25"></a>`query` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-3326d2ce08d0"></a>`quotas` | yes | type="array"; items=(#/components/schemas/KeyDownloadQuotaOut) |  |
| <a id="s-1460139393b8"></a>`sort` | yes | #/components/schemas/DownloadQuotaSort |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field quotas](#s-3326d2ce08d0) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-ea9fc70dcabd) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: DownloadQuotaSort](schemas-downloadquotasort.md)
- [schemas: KeyDownloadQuotaOut](schemas-keydownloadquotaout.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-9357afd753ce"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-426b6f6b3437"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb21)
- <a id="pa-e21b631a139d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
