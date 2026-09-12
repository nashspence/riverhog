# schemas: RegisterCollectionUploadSessionFilesRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-registercollectionuploadsessionfilesrequest:7f414b06b8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-d9e09e8c8862"></a>
- <a id="s-b486b1a7bf31"></a>`title`: RegisterCollectionUploadSessionFilesRequest
- <a id="s-e1210a6619af"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2885a9acd136"></a>`files` | yes | type="array"; minItems=1; maxItems=100; items=(#/components/schemas/CollectionUploadFileIn); additional keys=`x-riverhog-extent` |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)

Shared facts for every subject below: maximum=100; minimum=1; progression={"progression":"repeated-artifact-registration"}; reason="bounded-upload-registration"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-2885a9acd136) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionUploadFileIn](schemas-collectionuploadfilein.md)

## Governing policies

- <a id="pa-b856da54479f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-f64940feeb13"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RegisterCollectionUploadSessionFilesRequest`

### Exact owned JSON

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
