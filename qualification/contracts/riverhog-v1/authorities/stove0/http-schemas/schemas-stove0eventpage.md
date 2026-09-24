# schemas: Stove0EventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-stove0eventpage:af06a54c6e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-80bb6959f9"></a>

- <a id="s-c171ef6681"></a>`type`: `"object"`
- <a id="s-db171c377f"></a>`additionalProperties`: `false`
- <a id="s-f844ed6c6e"></a>`required`: `["events","next_cursor","has_more"]`
- <a id="s-01cef67a30"></a>`title`: `"Stove0EventPage"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-339a218ffa"></a>`events` | yes | type="array"; items=([Stove0LifecycleEvent](schemas-stove0lifecycleevent.md)); title="Events" |  |
| <a id="s-560f97dd63"></a>`has_more` | yes | type="boolean"; title="Has More" |  |
| <a id="s-88570f3326"></a>`next_cursor` | yes | type="string"; title="Next Cursor" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

Shared facts for every subject below: progression={"cursor_parameter":"after","kind":"cursor-feed","limit_parameter":"limit"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field events](#s-339a218ffa) | `cardinality · items · segmented_no_total_max` | shared above |

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

- [stove0-read-collection-progression/v1](../../../evidence/qualifications/stove0-read-collection-progression-v1/index.md)

## Maintained corroboration

### Referenced contract elements

- [Stove0LifecycleEvent](schemas-stove0lifecycleevent.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-ecae8ff75b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-31f4c1dd62"></a>[extent-rule/route-progression/v1](../../extent-contract/extent/extent-rule-route-progression.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/Stove0EventPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d8c540fb948db60713013d0ef55dbacdc480b5aefdf0df87abe2698a052140c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "events": {
      "items": {
        "$ref": "#/components/schemas/Stove0LifecycleEvent"
      },
      "title": "Events",
      "type": "array"
    },
    "has_more": {
      "title": "Has More",
      "type": "boolean"
    },
    "next_cursor": {
      "title": "Next Cursor",
      "type": "string"
    }
  },
  "required": [
    "events",
    "next_cursor",
    "has_more"
  ],
  "title": "Stove0EventPage",
  "type": "object"
}
```

</details>
