# schemas: ArtifactDispositionOutputDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-artifactdispositionoutputdocument:b331118743 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-6fe6290aa7"></a>

- <a id="s-3c6c99e709"></a>`type`: `"object"`
- <a id="s-534443c936"></a>`additionalProperties`: `false`
- <a id="s-b81d1fe26b"></a>`required`: `["input","output_path"]`
- <a id="s-763d3bd84d"></a>`title`: `"ArtifactDispositionOutputDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88ec4ab96c"></a>`input` | yes | [ArtifactDispositionInputDocument](schemas-artifactdispositioninputdocument.md) |  |
| <a id="s-26da0b1930"></a>`output_path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |

## Maintained corroboration

### Referenced contract elements

- [ArtifactDispositionInputDocument](schemas-artifactdispositioninputdocument.md)
- [CanonicalRelPath](schemas-canonicalrelpath.md)

## Governing policies

- <a id="pa-0d36d8d4ec"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionOutputDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
