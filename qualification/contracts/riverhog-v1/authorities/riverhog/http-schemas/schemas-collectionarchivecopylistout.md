# schemas: CollectionArchiveCopyListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionarchivecopylistout:e8170ae133 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-3dcc67425a"></a>

- <a id="s-ce2c8e495b"></a>`type`: `"object"`
- <a id="s-ef7d0388cd"></a>`additionalProperties`: `false`
- <a id="s-22bee6ad39"></a>`required`: `["collection_id","page_size","next_page_token","copies"]`
- <a id="s-73dd7cf3dc"></a>`title`: `"CollectionArchiveCopyListOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c4517a89e2"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-677882908c"></a>`copies` | yes | type="array"; items=(#/components/schemas/ArchiveCopyOut) |  |
| <a id="s-679b9d7139"></a>`next_page_token` | yes | anyOf=(#/components/schemas/BrowsePageToken) \| (type="null") |  |
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

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-read-collection-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-1536c4a29a)

## Maintained corroboration

### Referenced contract dossiers

- [ArchiveCopyOut](schemas-archivecopyout.md)
- [BrowsePageToken](schemas-browsepagetoken.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-fbf8d2abef"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-2a21c9054a"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-bd044f40f3"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
