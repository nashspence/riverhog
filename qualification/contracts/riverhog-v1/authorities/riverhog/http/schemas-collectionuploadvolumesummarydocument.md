# schemas: CollectionUploadVolumeSummaryDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadvolumesummarydocument:659de219af -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadVolumeSummaryDocument`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- `title`: CollectionUploadVolumeSummaryDocument
- `description`: Protocol-owned identity of one immutable collection archive volume.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `kind` | yes | string |  |
| `sequence` | yes | integer |  |
| `volume_id` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1ac7efaa0bbffcab4fff943fbbab4f3ed050c499a9fe1520ac38e1ccb0e6d859 -->

```json
{
  "additionalProperties": false,
  "description": "Protocol-owned identity of one immutable collection archive volume.",
  "properties": {
    "kind": {
      "enum": [
        "pack",
        "segment"
      ],
      "title": "Kind",
      "type": "string"
    },
    "sequence": {
      "minimum": 0,
      "title": "Sequence",
      "type": "integer"
    },
    "volume_id": {
      "pattern": "^(?:pack|segment)-[0-9a-f]{64}$",
      "title": "Volume Id",
      "type": "string"
    }
  },
  "required": [
    "volume_id",
    "sequence",
    "kind"
  ],
  "title": "CollectionUploadVolumeSummaryDocument",
  "type": "object"
}
```
