# schemas: ArtifactDispositionOutputDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionoutputdocument:78797035e0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6fe6290aa7"></a>
- <a id="s-763d3bd84d"></a>`title`: ArtifactDispositionOutputDocument
- <a id="s-3c6c99e709"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88ec4ab96c"></a>`input` | yes | #/components/schemas/ArtifactDispositionInputDocument |  |
| <a id="s-26da0b1930"></a>`output_path` | yes | #/components/schemas/CanonicalRelPath |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionInputDocument](schemas-artifactdispositioninputdocument.md)
- [schemas: CanonicalRelPath](schemas-canonicalrelpath.md)

## Governing policies

- <a id="pa-b50c847361"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
