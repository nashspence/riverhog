# schemas: RetrievalPlanFilePageOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalplanfilepageout:272d1680d1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-35d2ab1520"></a>

- <a id="s-ed412b4683"></a>`type`: `"object"`
- <a id="s-16dde3f70b"></a>`additionalProperties`: `false`
- <a id="s-e44c8a2127"></a>`required`: `["format","plan_id","etag","start_ordinal","complete","files"]`
- <a id="s-9d3361e816"></a>`title`: `"RetrievalPlanFilePageOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-328cd49682"></a>`complete` | yes | type="boolean"; title="Complete" |  |
| <a id="s-02762d6e31"></a>`etag` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Etag" |  |
| <a id="s-20cf25d13a"></a>`files` | yes | type="array"; items=([RetrievalPlanFileOut](schemas-retrievalplanfileout.md)); maxItems=100; title="Files" |  |
| <a id="s-3f4953179c"></a>`format` | yes | type="string"; const="riverhog-retrieval-plan-files/v1"; title="Format" |  |
| <a id="s-b41186f25a"></a>`next_ordinal` | no | anyOf=[(type="integer"; minimum=1; maximum=10000); (type="null")]; title="Next Ordinal" |  |
| <a id="s-a644293808"></a>`plan_id` | yes | type="string"; title="Plan Id" |  |
| <a id="s-8dc3c38e1e"></a>`start_ordinal` | yes | type="integer"; minimum=0; maximum=10000; title="Start Ordinal" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: maximum=100; progression={"authority":"retrieval-plan-files","cursor_parameter":"start_ordinal","kind":"exact-authority-page","limit_parameter":"page_size"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-20cf25d13a) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field etag](#s-02762d6e31) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-081802f705"></a>[field next_ordinal · integer value](#s-b41186f25a) | `value · schema-value · contract_max` | maximum=10000; minimum=1; reason="schema-maximum" |
| [field start_ordinal](#s-8dc3c38e1e) | `value · schema-value · contract_max` | maximum=10000; minimum=0; reason="schema-maximum" |

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

- [RetrievalPlanFileOut](schemas-retrievalplanfileout.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-9d9267ec31"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-6b86d22901"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)
- <a id="pa-8905b899ac"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalPlanFilePageOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bef4038a36e91bbf83a288398920aeeb633f4507becfdc7557f1fcb611608872 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "complete": {
      "title": "Complete",
      "type": "boolean"
    },
    "etag": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Etag",
      "type": "string"
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/RetrievalPlanFileOut"
      },
      "maxItems": 100,
      "title": "Files",
      "type": "array"
    },
    "format": {
      "const": "riverhog-retrieval-plan-files/v1",
      "title": "Format",
      "type": "string"
    },
    "next_ordinal": {
      "anyOf": [
        {
          "maximum": 10000,
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Next Ordinal"
    },
    "plan_id": {
      "title": "Plan Id",
      "type": "string"
    },
    "start_ordinal": {
      "maximum": 10000,
      "minimum": 0,
      "title": "Start Ordinal",
      "type": "integer"
    }
  },
  "required": [
    "format",
    "plan_id",
    "etag",
    "start_ordinal",
    "complete",
    "files"
  ],
  "title": "RetrievalPlanFilePageOut",
  "type": "object"
}
```

</details>
