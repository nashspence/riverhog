# schemas: AddCollectionUploadTagsRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-addcollectionuploadtagsrequest:f9db0ff120 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-725b5b627a"></a>
- <a id="s-00697fcd8b"></a>`title`: AddCollectionUploadTagsRequest
- <a id="s-d72c7ccc05"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7bad0c6531"></a>`tags` | yes | type="array"; minItems=1; maxItems=100; items=(#/components/schemas/CollectionTag); additional keys=`x-riverhog-extent` |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=100; minimum=1; progression={"progression":"repeat-request"}; reason="bounded-upload-staging-step; collection-tag-set-is-unbounded"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field tags](#s-7bad0c6531) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionTag](schemas-collectiontag.md)

## Governing policies

- <a id="pa-fedf9857ce"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-f01e1d1411"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
