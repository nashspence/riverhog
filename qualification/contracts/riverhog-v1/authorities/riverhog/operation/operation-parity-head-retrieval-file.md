# Operation parity: head_retrieval_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-head-retrieval-file:62bdd60426 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `retrieval-jobs` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/101`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | standard-tool/protocol |
| `cli_commands` | [] |
| `client` | None |
| `method` | HEAD |
| `operation_id` | head_retrieval_file |
| `path` | /v1/retrieval-jobs/{job_id}/content |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | stream-or-empty |
