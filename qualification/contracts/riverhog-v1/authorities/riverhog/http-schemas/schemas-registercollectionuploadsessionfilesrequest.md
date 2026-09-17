# schemas: RegisterCollectionUploadSessionFilesRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-registercollectionuploadsessionfilesrequest:7ff15ee883 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d9e09e8c88"></a>

- <a id="s-e1210a6619"></a>`type`: `"object"`
- <a id="s-cca069d17e"></a>`additionalProperties`: `false`
- <a id="s-8e46c1bcb1"></a>`required`: `["files"]`
- <a id="s-b486b1a7bf"></a>`title`: `"RegisterCollectionUploadSessionFilesRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2885a9acd1"></a>`files` | yes | type="array"; items=([CollectionUploadFileIn](schemas-collectionuploadfilein.md)); maxItems=100; minItems=1; title="Files"; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"repeated-artifact-registration","reason":"bounded-upload-registration"} |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=100; minimum=1; progression={"progression":"repeated-artifact-registration"}; reason="bounded-upload-registration"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-2885a9acd1) | `cardinality · items · segmented_no_total_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-upload-registration-progression/v1](../../../evidence/sources.md#e-5707b3a2d3-ca266cc9fb)

## Maintained corroboration

### Referenced contract dossiers

- [CollectionUploadFileIn](schemas-collectionuploadfilein.md)

## Governing policies

- <a id="pa-a962b5a5b0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-d840e806bc"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RegisterCollectionUploadSessionFilesRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
