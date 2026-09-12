# schemas: CollectionArchiveCopyListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionarchivecopylistout:da6e9b919d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-3dcc67425a"></a>
- <a id="s-73dd7cf3dc"></a>`title`: CollectionArchiveCopyListOut
- <a id="s-ce2c8e495b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c4517a89e2"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-677882908c"></a>`copies` | yes | type="array"; items=(#/components/schemas/ArchiveCopyOut) |  |
| <a id="s-679b9d7139"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-aef023ab6e"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field copies](#s-677882908c) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-aef023ab6e) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveCopyOut](schemas-archivecopyout.md)
- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-7efdd9e978"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-f174d7533a"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-4a5d81cddf"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionArchiveCopyListOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 32d0f1ea2a25e46086d96f1d485b97af83099433ef4dde85cfe972815dc0abb7 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "copies": {
      "items": {
        "$ref": "#/components/schemas/ArchiveCopyOut"
      },
      "title": "Copies",
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
    "page_size": {
      "maximum": 100,
      "minimum": 1,
      "title": "Page Size",
      "type": "integer"
    }
  },
  "required": [
    "collection_id",
    "page_size",
    "next_page_token",
    "copies"
  ],
  "title": "CollectionArchiveCopyListOut",
  "type": "object"
}
```
