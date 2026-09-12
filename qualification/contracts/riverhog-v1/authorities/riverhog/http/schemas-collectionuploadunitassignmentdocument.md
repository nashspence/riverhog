# schemas: CollectionUploadUnitAssignmentDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadunitassignmentdocument:3b1d8dc849 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadUnitAssignmentDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionUploadUnitWorkDocument](schemas-collectionuploadunitworkdocument.md)
- [schemas: CollectionUploadVolumeSummaryDocument](schemas-collectionuploadvolumesummarydocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: CollectionUploadUnitAssignmentDocument
- `description`: One bounded, immutable unit offered by an exact upload session.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `plan_sha256` | yes | string |  |
| `unit` | yes | #/components/schemas/CollectionUploadUnitWorkDocument |  |
| `volume` | yes | #/components/schemas/CollectionUploadVolumeSummaryDocument |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e281174c6520832c75ebed0cb4aa7f8b44365cf8b1745789293a46869d2a60d -->

```json
{
  "additionalProperties": false,
  "description": "One bounded, immutable unit offered by an exact upload session.",
  "properties": {
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "unit": {
      "$ref": "#/components/schemas/CollectionUploadUnitWorkDocument"
    },
    "volume": {
      "$ref": "#/components/schemas/CollectionUploadVolumeSummaryDocument"
    }
  },
  "required": [
    "volume",
    "plan_sha256",
    "unit"
  ],
  "title": "CollectionUploadUnitAssignmentDocument",
  "type": "object"
}
```
