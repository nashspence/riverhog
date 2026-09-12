# schemas: CollectionProvenanceVerificationJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionprovenanceverificationjobout:58529eb02d -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionProvenanceVerificationJobOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: CollectionProvenanceVerificationJobOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `attempts` | yes | integer |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `failure` | yes | object (2 fields) |  |
| `finished_at` | yes | object (2 fields) |  |
| `requested_at` | yes | string |  |
| `result` | yes | object (1 fields) |  |
| `started_at` | yes | object (2 fields) |  |
| `state` | yes | string |  |
