# schemas: ArtifactFactBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-artifactfactbinding:8f4aae20c1 -->

Locate subject-keyed records inside one observer's declared facts schema.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-ceab5b82fb"></a>

- <a id="s-3dec4792e9"></a>`type`: `"object"`
- <a id="s-267c3118f0"></a>`additionalProperties`: `false`
- <a id="s-e417b95fbe"></a>`description`: `"Locate subject-keyed records inside one observer's declared facts schema."`
- <a id="s-20e4539450"></a>`required`: `["records_pointer"]`
- <a id="s-62a5af5334"></a>`title`: `"ArtifactFactBinding"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ced3f84eac"></a>`artifact_id_pointer` | no | type="string"; default="/artifact_id"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Artifact Id Pointer" |  |
| <a id="s-1cd1f7bbbe"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Records Pointer" |  |

## Governing policies

- <a id="pa-ae4a6ba71c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactFactBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
