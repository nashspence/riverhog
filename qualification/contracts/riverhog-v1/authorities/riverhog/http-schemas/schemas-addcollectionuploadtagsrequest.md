# schemas: AddCollectionUploadTagsRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-addcollectionuploadtagsrequest:5643da2f81 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-725b5b627a"></a>

- <a id="s-d72c7ccc05"></a>`type`: `"object"`
- <a id="s-12e6c4f665"></a>`additionalProperties`: `false`
- <a id="s-3f7a93066b"></a>`required`: `["tags"]`
- <a id="s-00697fcd8b"></a>`title`: `"AddCollectionUploadTagsRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7bad0c6531"></a>`tags` | yes | type="array"; items=([CollectionTag](schemas-collectiontag.md)); maxItems=100; minItems=1; title="Tags"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"repeat-request","reason":"bounded-upload-staging-step; collection-tag-set-is-unbounded"} |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=100; minimum=1; progression={"progression":"repeat-request"}; reason="bounded-upload-staging-step; collection-tag-set-is-unbounded"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field tags](#s-7bad0c6531) | `cardinality · items · segmented_no_total_max` | shared above |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594).

Exact evidence groups for this contract element:

- [riverhog-upload-tag-staging-progression/v1](../../../evidence/qualifications/riverhog-upload-tag-staging-progression-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [CollectionTag](schemas-collectiontag.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-819785dc1b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-d390865946"></a>[extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AddCollectionUploadTagsRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 214539b2d2c07af11ffdb113fd836214811fd2a8395f88cd0be7e3e3bfd8fa3a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "tags": {
      "items": {
        "$ref": "#/components/schemas/CollectionTag"
      },
      "maxItems": 100,
      "minItems": 1,
      "title": "Tags",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "repeat-request",
        "reason": "bounded-upload-staging-step; collection-tag-set-is-unbounded"
      }
    }
  },
  "required": [
    "tags"
  ],
  "title": "AddCollectionUploadTagsRequest",
  "type": "object"
}
```

</details>
