# schemas: ArtifactDispositionSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactdispositionsetidentity:55812ad649 -->

Small identity for one sealed claim-scoped relational disposition set.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4b1b583c98b2"></a>
- <a id="s-e34cd61d43c0"></a>`title`: ArtifactDispositionSetIdentity
- <a id="s-77e2142af654"></a>`description`: Small identity for one sealed claim-scoped relational disposition set.
- <a id="s-4a0ec0fbada4"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8daa61628bf5"></a>`disposition_count` | yes | type="integer" |  |
| <a id="s-c8688cdd70ff"></a>`output_artifact_count` | yes | type="integer" |  |
| <a id="s-18bf6a6e7604"></a>`output_edge_count` | yes | type="integer" |  |
| <a id="s-9c8a61bcdba4"></a>`sha256` | yes | type="string" |  |

## Governing policies

- <a id="pa-64498bc9843e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactDispositionSetIdentity`

### Exact owned JSON

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
