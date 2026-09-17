# schemas: CollectionUploadWorkBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadworkbatchdocument:5afdc1051c -->

A bounded acquisition step over currently actionable upload units.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-7ff4df5056"></a>

- <a id="s-cd8632738e"></a>`type`: `"object"`
- <a id="s-a6cde8a9e3"></a>`additionalProperties`: `false`
- <a id="s-e3b8f0d45d"></a>`description`: `"A bounded acquisition step over currently actionable upload units."`
- <a id="s-4fe53b4041"></a>`required`: `["collection_id","planning_complete","complete","committed_payload_bytes","work"]`
- <a id="s-846950f272"></a>`title`: `"CollectionUploadWorkBatchDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0f1e40b93b"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-86f4d750af"></a>`committed_payload_bytes` | yes | type="integer"; minimum=0; title="Committed Payload Bytes" |  |
| <a id="s-9c4aca2092"></a>`complete` | yes | type="boolean"; title="Complete" |  |
| <a id="s-b5d3083403"></a>`planning_complete` | yes | type="boolean"; title="Planning Complete" |  |
| <a id="s-4617d6f48d"></a>`work` | yes | type="array"; items=([CollectionUploadUnitAssignmentDocument](schemas-collectionuploadunitassignmentdocument.md)); maxItems=64; title="Work"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"repeated-acquisition-until-complete","reason":"bounded-actionable-work-acquisition"} |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=64; minimum=null; progression={"progression":"repeated-acquisition-until-complete"}; reason="bounded-actionable-work-acquisition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field work](#s-4617d6f48d) | `cardinality · items · segmented_no_total_max` | shared above |

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

- [riverhog-upload-work-progression/v1](../../../evidence/qualifications/riverhog-upload-work-progression-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [CollectionUploadUnitAssignmentDocument](schemas-collectionuploadunitassignmentdocument.md)

## Governing policies

- <a id="pa-452b54bbba"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-c056756e50"></a>[extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadWorkBatchDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d4a81eca38f9ee605b1fa6d735fa52ca26238ca6b110b93a24ede24349ae9f2 -->

```json
{
  "additionalProperties": false,
  "description": "A bounded acquisition step over currently actionable upload units.",
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "committed_payload_bytes": {
      "minimum": 0,
      "title": "Committed Payload Bytes",
      "type": "integer"
    },
    "complete": {
      "title": "Complete",
      "type": "boolean"
    },
    "planning_complete": {
      "title": "Planning Complete",
      "type": "boolean"
    },
    "work": {
      "items": {
        "$ref": "#/components/schemas/CollectionUploadUnitAssignmentDocument"
      },
      "maxItems": 64,
      "title": "Work",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "repeated-acquisition-until-complete",
        "reason": "bounded-actionable-work-acquisition"
      }
    }
  },
  "required": [
    "collection_id",
    "planning_complete",
    "complete",
    "committed_payload_bytes",
    "work"
  ],
  "title": "CollectionUploadWorkBatchDocument",
  "type": "object"
}
```

</details>
