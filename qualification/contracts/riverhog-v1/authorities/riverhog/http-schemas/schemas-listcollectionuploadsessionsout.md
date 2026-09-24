# schemas: ListCollectionUploadSessionsOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-listcollectionuploadsessionsout:c7fe87e9cb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-68f12de12f"></a>

- <a id="s-dcd9a62569"></a>`type`: `"object"`
- <a id="s-298079091c"></a>`additionalProperties`: `false`
- <a id="s-73b499f21f"></a>`required`: `["page_size","next_page_token","sort","order","query","filters","uploads"]`
- <a id="s-069c01a536"></a>`title`: `"ListCollectionUploadSessionsOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-11a544ab1d"></a>`filters` | yes | [CollectionUploadListFiltersOut](schemas-collectionuploadlistfiltersout.md) |  |
| <a id="s-03b31c7b0d"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-56f15252a9"></a>`order` | yes | [SortOrder](schemas-sortorder.md) |  |
| <a id="s-72664f10d3"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-8149b52061"></a>`query` | yes | anyOf=[(type="string"); (type="null")]; title="Query" |  |
| <a id="s-7511b5b169"></a>`sort` | yes | [CollectionUploadSort](schemas-collectionuploadsort.md) |  |
| <a id="s-aed0553573"></a>`uploads` | yes | type="array"; items=([CollectionUploadListItemOut](schemas-collectionuploadlistitemout.md)); title="Uploads" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field uploads](#s-aed0553573) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-72664f10d3) | `value · schema-value · contract_max` | shared above |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb).

Exact evidence groups for this contract element:

- [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [BrowsePageToken](schemas-browsepagetoken.md)
- [CollectionUploadListFiltersOut](schemas-collectionuploadlistfiltersout.md)
- [CollectionUploadListItemOut](schemas-collectionuploadlistitemout.md)
- [CollectionUploadSort](schemas-collectionuploadsort.md)
- [SortOrder](schemas-sortorder.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b8ad658d2b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-3126ce0913"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-4dceaacafa"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListCollectionUploadSessionsOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 38271e9e5c7ff308105f9287d60cab0d21d596e89cfcb7949e09970921661ccd -->

```json
{
  "additionalProperties": false,
  "properties": {
    "filters": {
      "$ref": "#/components/schemas/CollectionUploadListFiltersOut"
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
      "$ref": "#/components/schemas/CollectionUploadSort"
    },
    "uploads": {
      "items": {
        "$ref": "#/components/schemas/CollectionUploadListItemOut"
      },
      "title": "Uploads",
      "type": "array"
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "filters",
    "uploads"
  ],
  "title": "ListCollectionUploadSessionsOut",
  "type": "object"
}
```

</details>
