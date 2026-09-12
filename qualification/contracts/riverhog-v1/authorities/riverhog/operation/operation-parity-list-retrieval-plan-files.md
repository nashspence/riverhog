# Operation parity: list_retrieval_plan_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-retrieval-plan-files:50e13d9b10 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `retrieval-plans` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/106`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/retrieval-plans/{plan_id}/files](../http/get-v1-retrieval-plans-plan-id-files.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | ["local repair", "local sync"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | list_retrieval_plan_files |
| `path` | /v1/retrieval-plans/{plan_id}/files |
| `provider_evidence` | None |
| `read_collection` | {"authority": "retrieval-plan-files", "cursor_parameter": "start_ordinal", "kind": "exact-authority-page", "limit_parameter": "page_size"} |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 043c8b8878386a291fe6560db9d407477a1c98d498251dcd6c183d41da197dbf -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_retrieval_plan_files",
  "path": "/v1/retrieval-plans/{plan_id}/files",
  "provider_evidence": null,
  "read_collection": {
    "authority": "retrieval-plan-files",
    "cursor_parameter": "start_ordinal",
    "kind": "exact-authority-page",
    "limit_parameter": "page_size"
  },
  "response_authority": "http-json"
}
```
