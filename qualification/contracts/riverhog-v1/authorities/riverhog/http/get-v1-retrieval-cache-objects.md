# GET /v1/retrieval-cache/objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-retrieval-cache-objects:91e5040e4e -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `retrieval-cache` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1retrieval-cache~1objects/get`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/route-progression/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: list_retrieval_cache_objects](../operation/operation-parity-list-retrieval-cache-objects.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| logical-result-cardinality | items | `segmented_no_total_max` | reason=bounded-route-progression |
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Contract

- `operationId`: list_retrieval_cache_objects
- `summary`: List Retrieval Cache Objects
- `security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `page_size` | query | no | integer |
| `page_token` | query | no | object (2 fields) |
| `q` | query | no | object (2 fields) |
| `collection_id` | query | no | object (2 fields) |
| `source_store` | query | no | object (2 fields) |
| `cache_store` | query | no | object (2 fields) |
| `state` | query | no | object (2 fields) |
| `protection` | query | no | object (2 fields) |
| `expires_before` | query | no | object (2 fields) |
| `expires_after` | query | no | object (2 fields) |
| `sort` | query | no | #/components/schemas/RetrievalCacheSort |
| `order` | query | no | #/components/schemas/SortOrder |

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `500` | Internal Server Error |
