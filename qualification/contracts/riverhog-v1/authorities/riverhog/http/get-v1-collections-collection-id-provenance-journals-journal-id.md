# GET /v1/collections/{collection_id}/provenance/journals/{journal_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:get-v1-collections-collection-id-provenan-3a9ce32406:fbdb931347 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collections~1{collection_id}~1provenance~1journals~1{journal_id}/get`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: stream_collection_provenance_journal](../operation/operation-parity-stream-collection-provenance-journal.md)

## Contract

- `operationId`: stream_collection_provenance_journal
- `summary`: Stream Collection Provenance Journal
- `security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `collection_id` | path | yes | integer |
| `journal_id` | path | yes | string |
| `Range` | header | no | object (2 fields) |
| `If-Match` | header | no | object (2 fields) |

### Responses

| Status | Description |
|---|---|
| `200` | Exact immutable provenance journal. |
| `206` | Exact immutable provenance journal byte range. |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `404` | Not Found |
| `412` | Precondition Failed |
| `428` | Precondition Required |
| `500` | Internal Server Error |
