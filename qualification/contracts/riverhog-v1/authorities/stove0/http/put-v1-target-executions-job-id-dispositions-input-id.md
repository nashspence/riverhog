# PUT /v1/target-executions/{job_id}/dispositions/{input_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:put-v1-target-executions-job-id-dispositi-608495d368:54f72585cb -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `target-executions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1target-executions~1{job_id}~1dispositions~1{input_id}/put`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: declare_target_execution_disposition](../operation/operation-parity-declare-target-execution-disposition.md)

## Contract

- `operationId`: declare_target_execution_disposition
- `summary`: Declare Target Execution Disposition

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `job_id` | path | yes | string |
| `input_id` | path | yes | string |

### Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/InputDispositionDeclaration"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `500` | Internal Server Error |
