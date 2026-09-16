# schemas: ArtifactDispositionSetIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-artifactdispositionsetidentitydocument:0034774c8d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-be7215dafa"></a>

- <a id="s-133ef058a4"></a>`type`: `"object"`
- <a id="s-c02787efa6"></a>`additionalProperties`: `false`
- <a id="s-f0b68ac1b5"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`
- <a id="s-6e86dffb56"></a>`title`: `"ArtifactDispositionSetIdentityDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1b9abf9f70"></a>`disposition_count` | yes | type="integer"; minimum=1; title="Disposition Count" |  |
| <a id="s-35aa7633ae"></a>`output_artifact_count` | yes | type="integer"; minimum=1; title="Output Artifact Count" |  |
| <a id="s-82bf7da7d8"></a>`output_edge_count` | yes | type="integer"; minimum=1; title="Output Edge Count" |  |
| <a id="s-56209a9460"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-56209a9460) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-e82799760e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-e73905525c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionSetIdentityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e58430ac8dbd95e932c4d52b253787a3a26e728ac17e0a7c2d640ae94904af8f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "disposition_count": {
      "minimum": 1,
      "title": "Disposition Count",
      "type": "integer"
    },
    "output_artifact_count": {
      "minimum": 1,
      "title": "Output Artifact Count",
      "type": "integer"
    },
    "output_edge_count": {
      "minimum": 1,
      "title": "Output Edge Count",
      "type": "integer"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "disposition_count",
    "output_edge_count",
    "output_artifact_count",
    "sha256"
  ],
  "title": "ArtifactDispositionSetIdentityDocument",
  "type": "object"
}
```

</details>
