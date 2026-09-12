# schemas: ArtifactDispositionOutputDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionoutputdocument:78797035e0 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionOutputDocument`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: ArtifactDispositionOutputDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `input` | yes | #/components/schemas/ArtifactDispositionInputDocument |  |
| `output_path` | yes | #/components/schemas/CanonicalRelPath |  |
