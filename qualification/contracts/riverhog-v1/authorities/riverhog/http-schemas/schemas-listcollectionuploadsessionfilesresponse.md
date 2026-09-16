# schemas: ListCollectionUploadSessionFilesResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-listcollectionuploadsessionfilesresponse:b96c93cd76 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-bb7532ef67"></a>

- <a id="s-266f4242ee"></a>`type`: `"object"`
- <a id="s-229e3ee5ea"></a>`additionalProperties`: `false`
- <a id="s-c896b7d9b9"></a>`required`: `["collection_id","page_size","next_page_token","files"]`
- <a id="s-2b167517c9"></a>`title`: `"ListCollectionUploadSessionFilesResponse"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-92b5d4df83"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-b0788847ab"></a>`files` | yes | type="array"; items=(#/components/schemas/CollectionUploadFileOut) |  |
| <a id="s-13e8266f55"></a>`next_page_token` | yes | anyOf=(#/components/schemas/BrowsePageToken) \| (type="null") |  |
| <a id="s-5d2c3734cd"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-b0788847ab) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-5d2c3734cd) | `value · schema-value · contract_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-read-collection-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-1536c4a29a)

## Maintained corroboration

### Referenced contract dossiers

- [BrowsePageToken](schemas-browsepagetoken.md)
- [CollectionId](schemas-collectionid.md)
- [CollectionUploadFileOut](schemas-collectionuploadfileout.md)

## Governing policies

- <a id="pa-dd86e1c775"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-7d50e3e9f1"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-3fbfc7c811"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListCollectionUploadSessionFilesResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e89541d5cbf7f7a0a8e2feaa403f3c83357fdb6beabcc85d398d7d7c90eeb2df -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/CollectionUploadFileOut"
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
    "files"
  ],
  "title": "ListCollectionUploadSessionFilesResponse",
  "type": "object"
}
```

</details>
