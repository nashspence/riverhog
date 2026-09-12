# schemas: AddCollectionUploadTagsRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-addcollectionuploadtagsrequest:f9db0ff120 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

- `title`: AddCollectionUploadTagsRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `tags` | yes | type="array"; minItems=1; maxItems=100; items=(#/components/schemas/CollectionTag); additional keys=`x-riverhog-extent` |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=100, minimum=1, reason=bounded-upload-staging-step; collection-tag-set-is-unbounded |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionTag](schemas-collectiontag.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AddCollectionUploadTagsRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 214539b2d2c07af11ffdb113fd836214811fd2a8395f88cd0be7e3e3bfd8fa3a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "tags": {
      "items": {
        "$ref": "#/components/schemas/CollectionTag"
      },
      "maxItems": 100,
      "minItems": 1,
      "title": "Tags",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "repeat-request",
        "reason": "bounded-upload-staging-step; collection-tag-set-is-unbounded"
      }
    }
  },
  "required": [
    "tags"
  ],
  "title": "AddCollectionUploadTagsRequest",
  "type": "object"
}
```
