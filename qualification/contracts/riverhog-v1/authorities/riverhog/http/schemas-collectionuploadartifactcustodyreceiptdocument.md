# schemas: CollectionUploadArtifactCustodyReceiptDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadartifactcustodyre-65e2ca0901:eabff5874e -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadArtifactCustodyReceiptDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: CollectionUploadArtifactCustodyReceiptDocument
- `description`: Exact safe-release evidence for one artifact in construction state.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_object_count` | yes | integer |  |
| `archive_object_set_sha256` | yes | string |  |
| `bytes` | yes | integer |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `format` | no | string |  |
| `path` | yes | string |  |
| `receipt_sha256` | yes | string |  |
| `sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cbed166882210f398141b295acf49805b9bc31439d54644fefc2289dbf88a8cb -->

```json
{
  "additionalProperties": false,
  "description": "Exact safe-release evidence for one artifact in construction state.",
  "properties": {
    "archive_object_count": {
      "minimum": 1,
      "title": "Archive Object Count",
      "type": "integer"
    },
    "archive_object_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Archive Object Set Sha256",
      "type": "string"
    },
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "format": {
      "const": "riverhog-artifact-custody-receipt/v1",
      "default": "riverhog-artifact-custody-receipt/v1",
      "title": "Format",
      "type": "string"
    },
    "path": {
      "title": "Path",
      "type": "string"
    },
    "receipt_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Receipt Sha256",
      "type": "string"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "path",
    "bytes",
    "sha256",
    "archive_object_count",
    "archive_object_set_sha256",
    "receipt_sha256"
  ],
  "title": "CollectionUploadArtifactCustodyReceiptDocument",
  "type": "object"
}
```
