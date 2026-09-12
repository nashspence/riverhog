# schemas: ArtifactRule

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-artifactrule:1d9ed84d81 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactRule`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: ArtifactRule
- `description`: Classify one path; first matching rule wins.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `glob` | no | string |  |
| `media_type` | no | object (2 fields) |  |
| `role` | no | string |  |
