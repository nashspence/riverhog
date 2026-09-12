# schemas: OperationProjection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-operationprojection:ef6a8f5504 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/OperationProjection`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: OperationProjection
- `description`: One declarative JSON-pointer copy into an operation request.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `destination` | yes | string |  |
| `destination_pointer` | yes | string |  |
| `source` | yes | string |  |
| `source_pointer` | yes | string |  |
