# schemas: CollectionTagListOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectiontaglistout:5e1f986868 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-25a6d1b785"></a>

- <a id="s-b805b67926"></a>`type`: `"object"`
- <a id="s-7ea6bd4451"></a>`additionalProperties`: `false`
- <a id="s-30279db14d"></a>`required`: `["collection_id","revision","tag_set_identity","page_size","next_page_token","tags"]`
- <a id="s-2a1a030eee"></a>`title`: `"CollectionTagListOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-44e9b6604c"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-21b67b2b68"></a>`next_page_token` | yes | anyOf=[([BrowsePageToken](schemas-browsepagetoken.md)); (type="null")] |  |
| <a id="s-ba445df547"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100; title="Page Size" |  |
| <a id="s-e903eeee44"></a>`revision` | yes | type="integer"; minimum=1; maximum=9007199254740991; title="Revision" |  |
| <a id="s-8ce7664492"></a>`tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Tag Set Identity" |  |
| <a id="s-fdac558d71"></a>`tags` | yes | type="array"; items=([CollectionTag](schemas-collectiontag.md)); title="Tags" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"authority":"collection-tag-set","authority_parameter":"tag_set_identity","cursor_parameter":"page_token","kind":"exact-authority-page","limit_parameter":"page_size"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field tags](#s-fdac558d71) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-ba445df547) | `value · schema-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum" |
| [field revision](#s-e903eeee44) | `value · schema-value · contract_max` | maximum=9007199254740991; minimum=1; reason="schema-maximum" |
| [field tag_set_identity](#s-8ce7664492) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

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
- [CollectionTag](schemas-collectiontag.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-664bff9a09"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-0675b19de9"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-125469f7bc"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTagListOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9842f4b5487bf8be7af46821a59efdf08f93bd5ad6368a3470be8094cac56011 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
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
    },
    "revision": {
      "maximum": 9007199254740991,
      "minimum": 1,
      "title": "Revision",
      "type": "integer"
    },
    "tag_set_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Tag Set Identity",
      "type": "string"
    },
    "tags": {
      "items": {
        "$ref": "#/components/schemas/CollectionTag"
      },
      "title": "Tags",
      "type": "array"
    }
  },
  "required": [
    "collection_id",
    "revision",
    "tag_set_identity",
    "page_size",
    "next_page_token",
    "tags"
  ],
  "title": "CollectionTagListOut",
  "type": "object"
}
```

</details>
