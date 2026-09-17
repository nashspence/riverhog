# schemas: RetrievalFileReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalfilereferencedocument:e174de7e2e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b861cf0a7e"></a>

- <a id="s-f848a70ebe"></a>`type`: `"object"`
- <a id="s-cf36673b7e"></a>`additionalProperties`: `false`
- <a id="s-492730ae58"></a>`required`: `["collection_id","path"]`
- <a id="s-4e48ede770"></a>`title`: `"RetrievalFileReferenceDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6ebb92d5d4"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-7f1d0c3d3d"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |

## Maintained corroboration

### Referenced contract dossiers

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-03b1be4776"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalFileReferenceDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
