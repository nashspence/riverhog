# schemas: ListCollectionsResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-listcollectionsresponse:fde38eb2a0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d9858fd321"></a>

- <a id="s-77f4f9b8da"></a>`type`: `"object"`
- <a id="s-c3f8441000"></a>`additionalProperties`: `false`
- <a id="s-121fc63c9c"></a>`required`: `["page_size","next_page_token","sort","order","query","encryption_format","passphrase_id","tags","collections"]`
- <a id="s-54809b9c43"></a>`title`: `"ListCollectionsResponse"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1d875f0d26"></a>`collections` | yes | type="array"; items=([CollectionSummaryOut](schemas-collectionsummaryout.md)); title="Collections" |  |
| <a id="s-a2ba0da479"></a>`encryption_format` | yes | anyOf=[(type="string"); (type="null")]; title="Encryption Format" |  |
| <a id="s-a18815df9e"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-e0b450d7ec"></a>`order` | yes | [SortOrder](schemas-sortorder.md) |  |
| <a id="s-f6b9eb37e9"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-585f47498a"></a>`passphrase_id` | yes | anyOf=[(type="string"); (type="null")]; title="Passphrase Id" |  |
| <a id="s-316d14721e"></a>`query` | yes | anyOf=[(type="string"); (type="null")]; title="Query" |  |
| <a id="s-15d2c2d924"></a>`sort` | yes | [CollectionSort](schemas-collectionsort.md) |  |
| <a id="s-3ce334b3ba"></a>`tags` | yes | type="array"; items=([CollectionTag](schemas-collectiontag.md)); maxItems=100; title="Tags"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-tag-selector-batch"} |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token","response_items_field":"collections"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field collections](#s-1d875f0d26) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-f6b9eb37e9) | `value · schema-value · contract_max` | minimum=1; reason="schema-maximum" |
| [field tags](#s-3ce334b3ba) | `cardinality · items · contract_max` | reason="bounded-exact-tag-selector-batch" |

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
- [CollectionSort](schemas-collectionsort.md)
- [CollectionSummaryOut](schemas-collectionsummaryout.md)
- [CollectionTag](schemas-collectiontag.md)
- [SortOrder](schemas-sortorder.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-364872d757"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-d834266707"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-31950cc906"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListCollectionsResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea51bf4002f224d45c514963b53be98ac4990d8f31436df87fabbb2fd3765c9d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collections": {
      "items": {
        "$ref": "#/components/schemas/CollectionSummaryOut"
      },
      "title": "Collections",
      "type": "array"
    },
    "encryption_format": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Encryption Format"
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
    "passphrase_id": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Passphrase Id"
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
      "$ref": "#/components/schemas/CollectionSort"
    },
    "tags": {
      "items": {
        "$ref": "#/components/schemas/CollectionTag"
      },
      "maxItems": 100,
      "title": "Tags",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-exact-tag-selector-batch"
      }
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "encryption_format",
    "passphrase_id",
    "tags",
    "collections"
  ],
  "title": "ListCollectionsResponse",
  "type": "object"
}
```

</details>
