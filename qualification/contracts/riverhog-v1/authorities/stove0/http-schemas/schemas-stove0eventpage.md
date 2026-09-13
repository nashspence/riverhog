# schemas: Stove0EventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-stove0eventpage:af06a54c6e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-80bb6959f9"></a>
- <a id="s-01cef67a30"></a>`title`: Stove0EventPage
- <a id="s-c171ef6681"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-339a218ffa"></a>`events` | yes | type="array"; items=(#/components/schemas/Stove0LifecycleEvent) |  |
| <a id="s-560f97dd63"></a>`has_more` | yes | type="boolean" |  |
| <a id="s-88570f3326"></a>`next_cursor` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

Shared facts for every subject below: progression={"cursor_parameter":"after","kind":"cursor-feed","limit_parameter":"limit"}; reason="bounded-route-page"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field events](#s-339a218ffa) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: Stove0LifecycleEvent](schemas-stove0lifecycleevent.md)

## Governing policies

- <a id="pa-ecae8ff75b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-31f4c1dd62"></a>[extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/Stove0EventPage`

### Exact owned JSON

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
