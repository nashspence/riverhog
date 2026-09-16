# schemas: RiverhogEventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-riverhogeventpage:51b0f49866 -->

Exact externally visible contract owned by this semantic dossier.

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

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"cursor_parameter":"after","kind":"cursor-feed","limit_parameter":"limit"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field events](#s-fcfd3592d1) | `cardinality · items · segmented_no_total_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-read-collection-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-1536c4a29a)

## Maintained corroboration

### Referenced contract dossiers

- [LifecycleEventCursor](schemas-lifecycleeventcursor.md)
- [RiverhogLifecycleEvent](schemas-riverhoglifecycleevent.md)

## Governing policies

- <a id="pa-5c4a8f803b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-365042f4b3"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
