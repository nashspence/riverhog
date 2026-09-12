# PATCH /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:patch-v1-collection-upload-sessions-colle-7eff7dfe3e:43f71e064a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/riverhog/paths/~1v1~1collection-upload-sessions~1{collection_id}~1provenance~1journals~1{journal_id}/patch`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Related interface records

- [Operation parity: append_collection_upload_session_provenance_journal](../operation/operation-parity-append-collection-upload-session-provenance-journal.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `contract_max` | maximum=1048576, minimum=1, reason=schema-maximum |

## Contract

- `operationId`: append_collection_upload_session_provenance_journal
- `summary`: Append Collection Upload Session Provenance Journal
- `security`: `[{"HTTPBearer": []}]`

### Parameters

| Name | In | Required | Schema |
|---|---|---:|---|
| `collection_id` | path | yes | integer |
| `journal_id` | path | yes | string |
| `Upload-Offset` | header | yes | integer |
| `Content-Length` | header | yes | integer |

### Request body

`{"content": {"application/json-seq": {"schema": {"format": "binary", "type": "string"}}}, "required": true}`

### Responses

| Status | Description |
|---|---|
| `200` | Successful Response |
| `400` | Bad Request |
| `401` | Unauthorized |
| `403` | Forbidden |
| `404` | Not Found |
| `409` | Conflict |
| `411` | Length Required |
| `500` | Internal Server Error |
