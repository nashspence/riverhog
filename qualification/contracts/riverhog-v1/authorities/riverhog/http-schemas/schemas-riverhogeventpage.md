# schemas: RiverhogEventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-riverhogeventpage:51b0f49866 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-2637f8f359"></a>

- <a id="s-f47e5da2d3"></a>`type`: `"object"`
- <a id="s-3ff8befa06"></a>`additionalProperties`: `false`
- <a id="s-13e328c69d"></a>`required`: `["events","next_cursor","has_more"]`
- <a id="s-03bcc2ca62"></a>`title`: `"RiverhogEventPage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fcfd3592d1"></a>`events` | yes | type="array"; items=([RiverhogLifecycleEvent](schemas-riverhoglifecycleevent.md)); title="Events" |  |
| <a id="s-3508bd425f"></a>`has_more` | yes | type="boolean"; title="Has More" |  |
| <a id="s-c6adcba3bb"></a>`next_cursor` | yes | [LifecycleEventCursor](schemas-lifecycleeventcursor.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"cursor_parameter":"after","kind":"cursor-feed","limit_parameter":"limit"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field events](#s-fcfd3592d1) | `cardinality · items · segmented_no_total_max` | shared above |

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

- [LifecycleEventCursor](schemas-lifecycleeventcursor.md)
- [RiverhogLifecycleEvent](schemas-riverhoglifecycleevent.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-5c4a8f803b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-365042f4b3"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RiverhogEventPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 208d1e3875fbbe12f9fc7bac4ed788355006eb1344dfe8490a86a725fb0cbc6b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "events": {
      "items": {
        "$ref": "#/components/schemas/RiverhogLifecycleEvent"
      },
      "title": "Events",
      "type": "array"
    },
    "has_more": {
      "title": "Has More",
      "type": "boolean"
    },
    "next_cursor": {
      "$ref": "#/components/schemas/LifecycleEventCursor"
    }
  },
  "required": [
    "events",
    "next_cursor",
    "has_more"
  ],
  "title": "RiverhogEventPage",
  "type": "object"
}
```

</details>
