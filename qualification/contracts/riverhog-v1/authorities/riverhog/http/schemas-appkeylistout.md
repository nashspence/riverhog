# schemas: AppKeyListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-appkeylistout:1054cad23d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-f599d8e707"></a>
- <a id="s-db307cfd05"></a>`title`: AppKeyListOut
- <a id="s-af7fa155ca"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0abf40b30c"></a>`active` | yes | anyOf=type="boolean" \| type="null" |  |
| <a id="s-7c7507b662"></a>`app` | yes | #/components/schemas/ApplicationName |  |
| <a id="s-5f3fe1ebaf"></a>`keys` | yes | type="array"; items=(#/components/schemas/AppKeyOut) |  |
| <a id="s-9101fa3350"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-98ec6db189"></a>`order` | yes | #/components/schemas/SortOrder |  |
| <a id="s-7f4f8c329b"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-9a1536a391"></a>`query` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-d0d9fa0407"></a>`sort` | yes | #/components/schemas/ApplicationKeySort |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field keys](#s-5f3fe1ebaf) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-7f4f8c329b) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: AppKeyOut](schemas-appkeyout.md)
- [schemas: ApplicationKeySort](schemas-applicationkeysort.md)
- [schemas: ApplicationName](schemas-applicationname.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-138abfc21a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-2af97b3ce4"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-c8ac712b74"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppKeyListOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d9d5d86ce8c6a02c518f63d7ac45c69a9f7b1ff2d382928792dc30b977f8bc4a -->

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
      "$ref": "#/components/schemas/ApplicationName"
    },
    "keys": {
      "items": {
        "$ref": "#/components/schemas/AppKeyOut"
      },
      "title": "Keys",
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
      "$ref": "#/components/schemas/ApplicationKeySort"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "active",
    "app",
    "keys"
  ],
  "title": "AppKeyListOut",
  "type": "object"
}
```
