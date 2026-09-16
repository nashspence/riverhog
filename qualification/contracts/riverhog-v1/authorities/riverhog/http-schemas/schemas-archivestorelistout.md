# schemas: ArchiveStoreListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivestorelistout:d3dcf7793a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8f8465c766"></a>

- <a id="s-7963ba868e"></a>`type`: `"object"`
- <a id="s-ba77ddafb4"></a>`additionalProperties`: `false`
- <a id="s-3624c7bd0c"></a>`required`: `["page_size","next_page_token","sort","order","query","stores"]`
- <a id="s-32226824e2"></a>`title`: `"ArchiveStoreListOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d9819f2441"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-9769555320"></a>`order` | yes | [SortOrder](schemas-sortorder.md) |  |
| <a id="s-5ff41a14f1"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-3f9aa6ec70"></a>`query` | yes | anyOf=[(type="string"); (type="null")]; title="Query" |  |
| <a id="s-a6a74ff330"></a>`sort` | yes | [ArchiveStoreSort](schemas-archivestoresort.md) |  |
| <a id="s-cfd4a736fc"></a>`stores` | yes | type="array"; items=([ArchiveStoreOut](schemas-archivestoreout.md)); title="Stores" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field stores](#s-cfd4a736fc) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-5ff41a14f1) | `value · schema-value · contract_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-read-collection-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-1536c4a29a)

## Maintained corroboration

### Referenced contract dossiers

- [ArchiveStoreOut](schemas-archivestoreout.md)
- [ArchiveStoreSort](schemas-archivestoresort.md)
- [BrowsePageToken](schemas-browsepagetoken.md)
- [SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-428663bab5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-9db6f974c2"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)
- <a id="pa-203bf4407e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveStoreListOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
