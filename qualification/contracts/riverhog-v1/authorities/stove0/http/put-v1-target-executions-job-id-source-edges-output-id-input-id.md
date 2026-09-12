# PUT /v1/target-executions/{job_id}/source-edges/{output_id}/{input_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:put-v1-target-executions-job-id-source-ed-4cec468e6f:fbe1589e58 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `target-executions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/paths/~1v1~1target-executions~1{job_id}~1source-edges~1{output_id}~1{input_id}/put`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: declare_target_execution_source_edge](../operation/operation-parity-declare-target-execution-source-edge.md)

## Contract

- `operationId`: declare_target_execution_source_edge
- `summary`: Declare Target Execution Source Edge

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `job_id` | path | yes | string |
| `output_id` | path | yes | string |
| `input_id` | path | yes | string |

### Request body

`{"content": {"application/json": {"schema": {"$ref": "#/components/schemas/OutputSourceEdge"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `500` | Internal Server Error |
