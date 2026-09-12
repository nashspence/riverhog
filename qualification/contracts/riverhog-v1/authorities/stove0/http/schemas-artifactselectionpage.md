# schemas: ArtifactSelectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactselectionpage:47cb134754 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactSelectionPage`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactSelectionRef](schemas-artifactselectionref.md)
- [schemas: ArtifactSubject](schemas-artifactsubject.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=256, reason=bounded-route-page |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ArtifactSelectionPage
- `description`: One bounded continuation step through an immutable artifact selection.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifacts` | yes | array |  |
| `authority` | yes | #/components/schemas/ArtifactSelectionRef |  |
| `complete` | yes | boolean |  |
| `continuation` | no | object (2 fields) |  |
| `next_continuation` | no | object (2 fields) |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02d0861511675ea88bb1eabb52ffb7a83cbbce075c250e5d031e6bf71a902702 -->

```json
{
  "additionalProperties": false,
  "description": "One bounded continuation step through an immutable artifact selection.",
  "properties": {
    "artifacts": {
      "items": {
        "$ref": "#/components/schemas/ArtifactSubject"
      },
      "maxItems": 256,
      "title": "Artifacts",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "selection-bound-start_ordinal",
        "reason": "bounded-artifact-selection-page"
      }
    },
    "authority": {
      "$ref": "#/components/schemas/ArtifactSelectionRef"
    },
    "complete": {
      "title": "Complete",
      "type": "boolean"
    },
    "continuation": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Continuation"
    },
    "next_continuation": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Next Continuation"
    }
  },
  "required": [
    "authority",
    "complete",
    "artifacts"
  ],
  "title": "ArtifactSelectionPage",
  "type": "object"
}
```
