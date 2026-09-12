# schemas: ArtifactDispositionPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionpagedocument:0f6d11c205 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionPageDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactDispositionDocument](schemas-artifactdispositiondocument.md)
- [schemas: ArtifactDispositionSetIdentityDocument](schemas-artifactdispositionsetidentitydocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=128, reason=bounded-route-page |

## Contract summary

- `title`: ArtifactDispositionPageDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `authority` | yes | #/components/schemas/ArtifactDispositionSetIdentityDocument |  |
| `dispositions` | yes | array |  |
| `next_ordinal` | no | object (2 fields) |  |
| `start_ordinal` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de49868fb7242eaf4c91278a54a47542b5a126d885b24423b420e2deb84be36b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authority": {
      "$ref": "#/components/schemas/ArtifactDispositionSetIdentityDocument"
    },
    "dispositions": {
      "items": {
        "$ref": "#/components/schemas/ArtifactDispositionDocument"
      },
      "maxItems": 128,
      "title": "Dispositions",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "authority-bound-start_ordinal",
        "reason": "bounded-authority-page"
      }
    },
    "next_ordinal": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "title": "Next Ordinal"
    },
    "start_ordinal": {
      "minimum": 0,
      "title": "Start Ordinal",
      "type": "integer"
    }
  },
  "required": [
    "authority",
    "start_ordinal",
    "dispositions"
  ],
  "title": "ArtifactDispositionPageDocument",
  "type": "object"
}
```
