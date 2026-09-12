# schemas: RegisterCollectionUploadSessionFilesRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-registercollectionuploadsessionfilesrequest:7f414b06b8 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RegisterCollectionUploadSessionFilesRequest`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionUploadFileIn](schemas-collectionuploadfilein.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=100, minimum=1, reason=bounded-upload-registration |

## Contract summary

- `title`: RegisterCollectionUploadSessionFilesRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `files` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b8ad7b15f6269dc58795f409f2f100545ba5b117958e211c90bcd689253ed24 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "files": {
      "items": {
        "$ref": "#/components/schemas/CollectionUploadFileIn"
      },
      "maxItems": 100,
      "minItems": 1,
      "title": "Files",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "repeated-artifact-registration",
        "reason": "bounded-upload-registration"
      }
    }
  },
  "required": [
    "files"
  ],
  "title": "RegisterCollectionUploadSessionFilesRequest",
  "type": "object"
}
```
