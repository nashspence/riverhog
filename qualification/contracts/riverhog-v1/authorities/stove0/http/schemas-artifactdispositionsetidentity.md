# schemas: ArtifactDispositionSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactdispositionsetidentity:55812ad649 -->

Small identity for one sealed claim-scoped relational disposition set.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: ArtifactDispositionSetIdentity
- `description`: Small identity for one sealed claim-scoped relational disposition set.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `disposition_count` | yes | type="integer" |  |
| `output_artifact_count` | yes | type="integer" |  |
| `output_edge_count` | yes | type="integer" |  |
| `sha256` | yes | type="string" |  |

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
