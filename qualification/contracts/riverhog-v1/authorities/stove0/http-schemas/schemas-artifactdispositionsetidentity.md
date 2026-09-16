# schemas: ArtifactDispositionSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-artifactdispositionsetidentity:c3da68138d -->

Small identity for one sealed claim-scoped relational disposition set.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4b1b583c98"></a>

- <a id="s-4a0ec0fbad"></a>`type`: `"object"`
- <a id="s-77e2142af6"></a>`description`: `"Small identity for one sealed claim-scoped relational disposition set."`
- <a id="s-df3ed3f41a"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`
- <a id="s-e34cd61d43"></a>`title`: `"ArtifactDispositionSetIdentity"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8daa61628b"></a>`disposition_count` | yes | type="integer" |  |
| <a id="s-c8688cdd70"></a>`output_artifact_count` | yes | type="integer" |  |
| <a id="s-18bf6a6e76"></a>`output_edge_count` | yes | type="integer" |  |
| <a id="s-9c8a61bcdb"></a>`sha256` | yes | type="string" |  |

## Governing policies

- <a id="pa-6c62414270"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactDispositionSetIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da785cc36d08d32c679afa80abf4b3cf2f569a2109a8cb53b212571d59d8b187 -->

```json
{
  "description": "Small identity for one sealed claim-scoped relational disposition set.",
  "properties": {
    "disposition_count": {
      "title": "Disposition Count",
      "type": "integer"
    },
    "output_artifact_count": {
      "title": "Output Artifact Count",
      "type": "integer"
    },
    "output_edge_count": {
      "title": "Output Edge Count",
      "type": "integer"
    },
    "sha256": {
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
  "title": "ArtifactDispositionSetIdentity",
  "type": "object"
}
```

</details>
