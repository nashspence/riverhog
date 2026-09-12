# schemas: RetrievalFileReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalfilereferencedocument:8d4d05d3c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b861cf0a7e"></a>
- <a id="s-4e48ede770"></a>`title`: RetrievalFileReferenceDocument
- <a id="s-f848a70ebe"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6ebb92d5d4"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-7f1d0c3d3d"></a>`path` | yes | #/components/schemas/CanonicalRelPath |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CanonicalRelPath](schemas-canonicalrelpath.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-c4adb9b169"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalFileReferenceDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f8f3e9fa619df11c44efee5673f38655aa2c7e6c6cc06c75f060a520cb75b148 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    }
  },
  "required": [
    "collection_id",
    "path"
  ],
  "title": "RetrievalFileReferenceDocument",
  "type": "object"
}
```
