# schemas: CollectionUploadRawPartsIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadrawpartsin:6bc3d512dc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c3dd20ed1afe"></a>
- <a id="s-d2abbc926ca1"></a>`title`: CollectionUploadRawPartsIn
- <a id="s-1fb2218631d6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-63f2f80203a9"></a>`ordered_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f5f8f9f00b60"></a>`part_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-ad73d85dc2e8"></a>`part_plaintext_bytes` | yes | type="integer"; minimum=65536 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field ordered_sha256](#s-63f2f80203a9) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-d02bb1ad05d1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-2c51eaed595c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadRawPartsIn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 76ccb435180670cd87a5f21b23e35a4dfbc75f28f4535248bc6671c05f6a8baf -->

```json
{
  "additionalProperties": false,
  "properties": {
    "ordered_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Ordered Sha256",
      "type": "string"
    },
    "part_count": {
      "minimum": 1,
      "title": "Part Count",
      "type": "integer"
    },
    "part_plaintext_bytes": {
      "minimum": 65536,
      "title": "Part Plaintext Bytes",
      "type": "integer"
    }
  },
  "required": [
    "part_plaintext_bytes",
    "part_count",
    "ordered_sha256"
  ],
  "title": "CollectionUploadRawPartsIn",
  "type": "object"
}
```
