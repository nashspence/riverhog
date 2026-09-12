# schemas: ArtifactDispositionSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactdispositionsetidentity:55812ad649 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactDispositionSetIdentity`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: ArtifactDispositionSetIdentity
- `description`: Small identity for one sealed claim-scoped relational disposition set.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `disposition_count` | yes | integer |  |
| `output_artifact_count` | yes | integer |  |
| `output_edge_count` | yes | integer |  |
| `sha256` | yes | string |  |
