# schemas: CollectionUploadRawDigestProgressDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadrawdigestprogressdocument:7173619fcf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-af655bffa6"></a>
- <a id="s-0ebbaa177b"></a>`title`: CollectionUploadRawDigestProgressDocument
- <a id="s-eee2c580c6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-357edf47b9"></a>`accepted_parts` | yes | type="integer"; minimum=0 |  |
| <a id="s-d4a25895d4"></a>`complete` | yes | type="boolean" |  |
| <a id="s-87d83509ec"></a>`expected_parts` | yes | type="integer"; minimum=1 |  |
| <a id="s-1e7da99a8e"></a>`path` | yes | type="string" |  |

## Governing policies

- <a id="pa-6c71a4853b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadRawDigestProgressDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 65617a770d19d14c6eaf53cf21c753767836d93c430135b0465dcfe6d6f219bd -->

```json
{
  "additionalProperties": false,
  "properties": {
    "accepted_parts": {
      "minimum": 0,
      "title": "Accepted Parts",
      "type": "integer"
    },
    "complete": {
      "title": "Complete",
      "type": "boolean"
    },
    "expected_parts": {
      "minimum": 1,
      "title": "Expected Parts",
      "type": "integer"
    },
    "path": {
      "title": "Path",
      "type": "string"
    }
  },
  "required": [
    "path",
    "accepted_parts",
    "expected_parts",
    "complete"
  ],
  "title": "CollectionUploadRawDigestProgressDocument",
  "type": "object"
}
```
