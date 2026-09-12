# schemas: AddCollectionUploadTagsRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-addcollectionuploadtagsrequest:f9db0ff120 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AddCollectionUploadTagsRequest`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CollectionTag](schemas-collectiontag.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `segmented_no_total_max` | maximum=100, minimum=1, reason=bounded-upload-staging-step; collection-tag-set-is-unbounded |

## Contract summary

- `title`: AddCollectionUploadTagsRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `tags` | yes | array |  |

## Complete owned contract

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
