# Operation parity: download_retrieval_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-download-retrieval-file:7a482c2b2b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `retrieval-jobs` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/100`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/retrieval-jobs/{job_id}/content](../http/get-v1-retrieval-jobs-job-id-content.md)
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
| `operation_id` | download_retrieval_file |
| `path` | /v1/retrieval-jobs/{job_id}/content |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | stream-or-empty |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e58d21f1ee66619f35c79ccfa757b30b5998a809ded57441bad96a46906e840e -->

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
  "operation_id": "download_retrieval_file",
  "path": "/v1/retrieval-jobs/{job_id}/content",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "stream-or-empty"
}
```
