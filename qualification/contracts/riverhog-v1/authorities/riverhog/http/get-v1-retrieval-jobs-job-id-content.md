# GET /v1/retrieval-jobs/{job_id}/content

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-retrieval-jobs-job-id-content:2b8e9d02d1 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `retrieval-jobs` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-jobs~1{job_id}~1content/get`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: download_retrieval_file](../operation/operation-parity-download-retrieval-file.md)

## Contract

- `operationId`: download_retrieval_file
- `summary`: Download Retrieval File
- `security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `job_id` | path | yes | string |
| `collection_id` | query | yes | #/components/schemas/CollectionIdParameter |
| `path` | query | yes | string |
| `If-Match` | header | yes | string |
| `Range` | header | no | object (2 fields) |
| `If-None-Match` | header | no | object (2 fields) |

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `404` | Not Found |
| `409` | Conflict |
| `412` | Precondition Failed |
| `416` | Requested Range Not Satisfiable |
| `429` | Too Many Requests |
| `500` | Internal Server Error |
