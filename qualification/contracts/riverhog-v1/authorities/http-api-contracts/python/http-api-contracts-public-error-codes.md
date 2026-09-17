# http_api_contracts.PUBLIC_ERROR_CODES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-public-error-codes:d620ff0805 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-acfaf814ab"></a>
- <a id="s-83ee07ecd6"></a>`distribution`: `http-api-contracts`
- <a id="s-8291f601f3"></a>`module`: `http_api_contracts`
- <a id="s-a9785197f2"></a>`name`: `PUBLIC_ERROR_CODES`
- <a id="s-30aab8b80e"></a>`unit`: `export`

### Declared structure

- <a id="s-1963b02387"></a>`kind`: `"constant"`
- <a id="s-b21efdef50"></a>`value`: `["bad_request","catalog_sync_cursor_expired","catalog_sync_history_expired","catalog_sync_source_changed","catalog_sync_view_changed","conflict","download_allowance_exceeded","forbidden","hash_mismatch","ingress_failed","input_upload_storage_hint_invalid","insufficient_storage","internal_error","invalid_path","invalid_range","invalid_state","invalid_target","job_template_revision_conflict","length_required","method_not_allowed","not_found","precondition_failed","precondition_required","service_unavailable","storage_hint_mismatch","submission_conflict","too_many_active_input_uploads","unauthorized"]`

## Governing policies

- <a id="pa-b33968b967"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.PUBLIC_ERROR_CODES`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b81b10cca493b64d88309da752128fd8e470bcf3dd125ee8e8de2c754a56bfd3 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": [
      "bad_request",
      "catalog_sync_cursor_expired",
      "catalog_sync_history_expired",
      "catalog_sync_source_changed",
      "catalog_sync_view_changed",
      "conflict",
      "download_allowance_exceeded",
      "forbidden",
      "hash_mismatch",
      "ingress_failed",
      "input_upload_storage_hint_invalid",
      "insufficient_storage",
      "internal_error",
      "invalid_path",
      "invalid_range",
      "invalid_state",
      "invalid_target",
      "job_template_revision_conflict",
      "length_required",
      "method_not_allowed",
      "not_found",
      "precondition_failed",
      "precondition_required",
      "service_unavailable",
      "storage_hint_mismatch",
      "submission_conflict",
      "too_many_active_input_uploads",
      "unauthorized"
    ]
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "PUBLIC_ERROR_CODES",
  "unit": "export"
}
```

</details>
