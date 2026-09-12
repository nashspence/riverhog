# schemas: ArtifactDispositionOutputDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionoutputdocument:78797035e0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: ArtifactDispositionOutputDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `input` | yes | #/components/schemas/ArtifactDispositionInputDocument |  |
| `output_path` | yes | #/components/schemas/CanonicalRelPath |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionInputDocument](schemas-artifactdispositioninputdocument.md)
- [schemas: CanonicalRelPath](schemas-canonicalrelpath.md)

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

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionOutputDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25a191e6c3727e1412f128c608919ce9f585a2394ba042b73150e611b36c98ea -->

```json
{
  "additionalProperties": false,
  "properties": {
    "input": {
      "$ref": "#/components/schemas/ArtifactDispositionInputDocument"
    },
    "output_path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    }
  },
  "required": [
    "input",
    "output_path"
  ],
  "title": "ArtifactDispositionOutputDocument",
  "type": "object"
}
```
