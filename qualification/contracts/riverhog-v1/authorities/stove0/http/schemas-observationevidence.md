# schemas: ObservationEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observationevidence:7cfc081818 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObservationEvidence`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: ObservationEvidence
- `description`: Complete routing evidence: immutable request plus accepted result.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `request` | yes | #/components/schemas/ObservationRequest |  |
| `result` | yes | #/components/schemas/ObservationResult |  |
