# schemas: SearchOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-searchout:76beb565b9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-825bcc743e"></a>

- <a id="s-fa94986254"></a>`type`: `"object"`
- <a id="s-984d70c84b"></a>`additionalProperties`: `false`
- <a id="s-4c3cbfbe16"></a>`required`: `["query","collection","page_size","next_page_token","sort","order","files"]`
- <a id="s-05ddf1c3f3"></a>`title`: `"SearchOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-644f7219db"></a>`collection` | yes | anyOf=[([CollectionId](schemas-collectionid.md)); (type="null")] |  |
| <a id="s-24fe0513e1"></a>`files` | yes | type="array"; items=([SearchFileOut](schemas-searchfileout.md)); title="Files" |  |
| <a id="s-92cc67f784"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-0e9603d9b8"></a>`order` | yes | [SortOrder](schemas-sortorder.md) |  |
| <a id="s-7cb5974c67"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-2298fc4e83"></a>`query` | yes | anyOf=[(type="string"); (type="null")]; title="Query" |  |
| <a id="s-1263e61e54"></a>`sort` | yes | [SearchSort](schemas-searchsort.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"default_page_size":25,"kind":"mutable-browse","maximum_page_size":100,"next_page_token_field":"next_page_token","page_size_parameter":"page_size","page_token_parameter":"page_token"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-24fe0513e1) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-7cb5974c67) | `value · schema-value · contract_max` | shared above |

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
- [CollectionId](schemas-collectionid.md)
- [SearchFileOut](schemas-searchfileout.md)
- [SearchSort](schemas-searchsort.md)
- [SortOrder](schemas-sortorder.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c668bd02ba"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-4bb868c171"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-21d1360a52"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/SearchOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3fb6499b75844aedb1236b5863237c3aadaee4458f3aea823529b3282ffde76e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionId"
        },
        {
          "type": "null"
        }
      ]
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/SearchFileOut"
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
      "$ref": "#/components/schemas/SearchSort"
    }
  },
  "required": [
    "query",
    "collection",
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "files"
  ],
  "title": "SearchOut",
  "type": "object"
}
```

</details>
