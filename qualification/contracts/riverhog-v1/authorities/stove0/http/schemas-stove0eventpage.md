# schemas: Stove0EventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-stove0eventpage:c5a03be3ff -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/Stove0EventPage`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: Stove0LifecycleEvent](schemas-stove0lifecycleevent.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | reason=bounded-route-page |

## Contract summary

- `title`: Stove0EventPage
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `events` | yes | array |  |
| `has_more` | yes | boolean |  |
| `next_cursor` | yes | string |  |

## Complete owned contract

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
