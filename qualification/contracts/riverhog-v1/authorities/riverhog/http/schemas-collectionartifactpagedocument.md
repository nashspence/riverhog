# schemas: CollectionArtifactPageDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionartifactpagedocument:67e047d193 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionArtifactPageDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactSetAuthorityDocument](schemas-artifactsetauthoritydocument.md)
- [schemas: CollectionArtifactIdentityDocument](schemas-collectionartifactidentitydocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=128, reason=bounded-route-page |

## Contract summary

- `title`: CollectionArtifactPageDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifacts` | yes | array |  |
| `authority` | yes | #/components/schemas/ArtifactSetAuthorityDocument |  |
| `next_ordinal` | no | object (2 fields) |  |
| `start_ordinal` | yes | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d36dba622d1c18ba09c32f815f78c7a8f4a1995796134145366ce2ddaee0d3d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifacts": {
      "items": {
        "$ref": "#/components/schemas/CollectionArtifactIdentityDocument"
      },
      "maxItems": 128,
      "title": "Artifacts",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "authority-bound-start_ordinal",
        "reason": "bounded-authority-page"
      }
    },
    "authority": {
      "$ref": "#/components/schemas/ArtifactSetAuthorityDocument"
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
    "artifacts"
  ],
  "title": "CollectionArtifactPageDocument",
  "type": "object"
}
```
