# schemas: RiverhogEventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-riverhogeventpage:3bdc142bc8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-2637f8f359"></a>
- <a id="s-03bcc2ca62"></a>`title`: RiverhogEventPage
- <a id="s-f47e5da2d3"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fcfd3592d1"></a>`events` | yes | type="array"; items=(#/components/schemas/RiverhogLifecycleEvent) |  |
| <a id="s-3508bd425f"></a>`has_more` | yes | type="boolean" |  |
| <a id="s-c6adcba3bb"></a>`next_cursor` | yes | #/components/schemas/LifecycleEventCursor |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"cursor_parameter":"after","kind":"cursor-feed","limit_parameter":"limit"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field events](#s-fcfd3592d1) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: LifecycleEventCursor](schemas-lifecycleeventcursor.md)
- [schemas: RiverhogLifecycleEvent](schemas-riverhoglifecycleevent.md)

## Governing policies

- <a id="pa-49ff7071b1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-06fe2b81df"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

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
