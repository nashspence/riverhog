# schemas: CoordinationCollectionResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-coordinationcollectionresult:63aad8c924 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CoordinationCollectionResult`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactSelectionRef](schemas-artifactselectionref.md)
- [schemas: CollectionRootRef](schemas-collectionrootref.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: CoordinationCollectionResult
- `description`: Parent-visible collection produced by the coordinator's actual join leaf.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `derivation_sha256` | yes | string |  |
| `join_settlement_sha256` | yes | string |  |
| `output_collection` | yes | #/components/schemas/CollectionRootRef |  |
| `output_selection` | yes | #/components/schemas/ArtifactSelectionRef |  |
| `producer_work_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 820ceced08fd62e760ccc86af09c0c0853234f5a2890ee7e56de01a2420d37ce -->

```json
{
  "additionalProperties": false,
  "description": "Parent-visible collection produced by the coordinator's actual join leaf.",
  "properties": {
    "derivation_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Derivation Sha256",
      "type": "string"
    },
    "join_settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Join Settlement Sha256",
      "type": "string"
    },
    "output_collection": {
      "$ref": "#/components/schemas/CollectionRootRef"
    },
    "output_selection": {
      "$ref": "#/components/schemas/ArtifactSelectionRef"
    },
    "producer_work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Producer Work Id",
      "type": "string"
    }
  },
  "required": [
    "producer_work_id",
    "join_settlement_sha256",
    "derivation_sha256",
    "output_collection",
    "output_selection"
  ],
  "title": "CoordinationCollectionResult",
  "type": "object"
}
```
