# POST /v1/evaluations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:post-v1-evaluations:0bc922d778 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `evaluations` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1evaluations/post`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: create_evaluation](../operation/operation-parity-create-evaluation.md)

## Contract

- `operationId`: create_evaluation
- `summary`: Create Evaluation

### Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/EvaluationDefinition"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| `201` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `409` | Conflict |
| `500` | Internal Server Error |
