# schemas: RiverhogEventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-riverhogeventpage:3bdc142bc8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: RiverhogEventPage
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `events` | yes | type="array"; items=(#/components/schemas/RiverhogLifecycleEvent) |  |
| `has_more` | yes | type="boolean" |  |
| `next_cursor` | yes | #/components/schemas/LifecycleEventCursor |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: LifecycleEventCursor](schemas-lifecycleeventcursor.md)
- [schemas: RiverhogLifecycleEvent](schemas-riverhoglifecycleevent.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
