# schemas: CollectionUploadUnitAssignmentDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadunitassignmentdocument:3b1d8dc849 -->

One bounded, immutable unit offered by an exact upload session.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-2964a9537359"></a>
- <a id="s-10c77c82e33b"></a>`title`: CollectionUploadUnitAssignmentDocument
- <a id="s-71d774a36600"></a>`description`: One bounded, immutable unit offered by an exact upload session.
- <a id="s-2ed2603f50e9"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5c08c11565f1"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bac5bfebcca3"></a>`unit` | yes | #/components/schemas/CollectionUploadUnitWorkDocument |  |
| <a id="s-f21e6e665e46"></a>`volume` | yes | #/components/schemas/CollectionUploadVolumeSummaryDocument |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field plan_sha256](#s-5c08c11565f1) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionUploadUnitWorkDocument](schemas-collectionuploadunitworkdocument.md)
- [schemas: CollectionUploadVolumeSummaryDocument](schemas-collectionuploadvolumesummarydocument.md)

## Governing policies

- <a id="pa-4f311fc5c3c0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-a1420fa6f958"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadUnitAssignmentDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e281174c6520832c75ebed0cb4aa7f8b44365cf8b1745789293a46869d2a60d -->

```json
{
  "additionalProperties": false,
  "description": "One bounded, immutable unit offered by an exact upload session.",
  "properties": {
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "unit": {
      "$ref": "#/components/schemas/CollectionUploadUnitWorkDocument"
    },
    "volume": {
      "$ref": "#/components/schemas/CollectionUploadVolumeSummaryDocument"
    }
  },
  "required": [
    "volume",
    "plan_sha256",
    "unit"
  ],
  "title": "CollectionUploadUnitAssignmentDocument",
  "type": "object"
}
```
