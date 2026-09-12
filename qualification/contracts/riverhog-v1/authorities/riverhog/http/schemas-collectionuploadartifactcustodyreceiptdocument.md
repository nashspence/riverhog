# schemas: CollectionUploadArtifactCustodyReceiptDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadartifactcustodyre-65e2ca0901:eabff5874e -->

Exact safe-release evidence for one artifact in construction state.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-de01edc6c7f6"></a>
- <a id="s-991238a92bf9"></a>`title`: CollectionUploadArtifactCustodyReceiptDocument
- <a id="s-881a8cdfbb91"></a>`description`: Exact safe-release evidence for one artifact in construction state.
- <a id="s-7ba375dfb50b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a0567d04d685"></a>`archive_object_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-652d1bb86ced"></a>`archive_object_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5ff22aaac293"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-48eeb34d300e"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-db6cbedc39b3"></a>`format` | no | type="string"; const="riverhog-artifact-custody-receipt/v1" |  |
| <a id="s-478a27e2aa82"></a>`path` | yes | type="string" |  |
| <a id="s-0ee690ffb923"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c54e529e154e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-5ff22aaac293) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_object_set_sha256](#s-652d1bb86ced) | `length · characters · fixed` | shared above |
| [field receipt_sha256](#s-0ee690ffb923) | `length · characters · fixed` | shared above |
| [field sha256](#s-c54e529e154e) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-08f09f974507"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-d3744d68e369"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-3285537fd680"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadArtifactCustodyReceiptDocument`

### Exact owned JSON

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
