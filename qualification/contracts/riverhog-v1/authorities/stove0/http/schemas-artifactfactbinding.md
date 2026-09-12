# schemas: ArtifactFactBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactfactbinding:6b436b0c1a -->

Locate subject-keyed records inside one observer's declared facts schema.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ceab5b82fb"></a>
- <a id="s-62a5af5334"></a>`title`: ArtifactFactBinding
- <a id="s-e417b95fbe"></a>`description`: Locate subject-keyed records inside one observer's declared facts schema.
- <a id="s-3dec4792e9"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ced3f84eac"></a>`artifact_id_pointer` | no | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-1cd1f7bbbe"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

## Governing policies

- <a id="pa-1b08929454"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactFactBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be2e042f0f939418ce250f41c4b0fb8be5f7c53668a50eff8f53679cc92e6567 -->

```json
{
  "additionalProperties": false,
  "description": "Locate subject-keyed records inside one observer's declared facts schema.",
  "properties": {
    "artifact_id_pointer": {
      "default": "/artifact_id",
      "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
      "title": "Artifact Id Pointer",
      "type": "string"
    },
    "records_pointer": {
      "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
      "title": "Records Pointer",
      "type": "string"
    }
  },
  "required": [
    "records_pointer"
  ],
  "title": "ArtifactFactBinding",
  "type": "object"
}
```
