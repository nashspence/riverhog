# schemas: CollectionUploadRawDigestProgressDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadrawdigestprogressdocument:7173619fcf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: CollectionUploadRawDigestProgressDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `accepted_parts` | yes | type="integer"; minimum=0 |  |
| `complete` | yes | type="boolean" |  |
| `expected_parts` | yes | type="integer"; minimum=1 |  |
| `path` | yes | type="string" |  |

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
