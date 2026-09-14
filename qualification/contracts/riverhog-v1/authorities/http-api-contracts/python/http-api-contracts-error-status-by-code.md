# http_api_contracts.ERROR_STATUS_BY_CODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-error-status-by-code:dde6f7c3db -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eb0c11ca71"></a>
- <a id="s-a60b1b6913"></a>`distribution`: `http-api-contracts`
- <a id="s-83ba7b67bc"></a>`module`: `http_api_contracts`
- <a id="s-b9e657f63f"></a>`name`: `ERROR_STATUS_BY_CODE`
- <a id="s-81891cbbfc"></a>`unit`: `export`

### Declared structure

- <a id="s-a203e97113"></a>`kind`: `"constant"`
- <a id="s-a49a77fd41"></a>`value`: `{"bad_request":400,"catalog_sync_cursor_expired":410,"catalog_sync_history_expired":410,"catalog_sync_source_changed":409,"catalog_sync_view_changed":409,"conflict":409,"download_allowance_exceeded":429,"forbidden":403,"hash_mismatch":409,"ingress_failed":500,"input_upload_storage_hint_invalid":409,"insufficient_storage":507,"internal_error":500,"invalid_path":400,"invalid_range":416,"invalid_state":409,"invalid_target":400,"job_template_revision_conflict":409,"length_required":411,"method_not_allowed":405,"not_found":404,"precondition_failed":412,"precondition_required":428,"service_unavailable":503,"storage_hint_mismatch":409,"submission_conflict":409,"too_many_active_input_uploads":429,"unauthorized":401}`

## Governing policies

- <a id="pa-ac6ffe89a7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.ERROR_STATUS_BY_CODE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5bd847a3f966cc268bf8a41026aa35e9e83a1aae6863d35d0e06f5f8cd2f27c9 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": {
      "bad_request": 400,
      "catalog_sync_cursor_expired": 410,
      "catalog_sync_history_expired": 410,
      "catalog_sync_source_changed": 409,
      "catalog_sync_view_changed": 409,
      "conflict": 409,
      "download_allowance_exceeded": 429,
      "forbidden": 403,
      "hash_mismatch": 409,
      "ingress_failed": 500,
      "input_upload_storage_hint_invalid": 409,
      "insufficient_storage": 507,
      "internal_error": 500,
      "invalid_path": 400,
      "invalid_range": 416,
      "invalid_state": 409,
      "invalid_target": 400,
      "job_template_revision_conflict": 409,
      "length_required": 411,
      "method_not_allowed": 405,
      "not_found": 404,
      "precondition_failed": 412,
      "precondition_required": 428,
      "service_unavailable": 503,
      "storage_hint_mismatch": 409,
      "submission_conflict": 409,
      "too_many_active_input_uploads": 429,
      "unauthorized": 401
    }
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "ERROR_STATUS_BY_CODE",
  "unit": "export"
}
```
