# Operation parity: cancel_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-cancel-retrieval-job:a0a433d9c6 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `retrieval-jobs` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/97`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [DELETE /v1/retrieval-jobs/{job_id}](../http/delete-v1-retrieval-jobs-job-id.md)
- [piggity local evict](../../piggity/cli/piggity-local-evict.md)
- [piggity local remove](../../piggity/cli/piggity-local-remove.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | ["local evict", "local remove"] |
| `client` | ApiClient |
| `method` | DELETE |
| `operation_id` | cancel_retrieval_job |
| `path` | /v1/retrieval-jobs/{job_id} |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |
