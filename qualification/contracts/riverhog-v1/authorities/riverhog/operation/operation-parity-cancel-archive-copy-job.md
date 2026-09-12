# Operation parity: cancel_archive_copy_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-cancel-archive-copy-job:10b40b9177 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `archive` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/16`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [DELETE /v1/archive/copies/{collection_id}/{destination_store}](../http/delete-v1-archive-copies-collection-id-destination-store.md)
- [piggity archive copy cancel](../../piggity/cli/piggity-archive-copy-cancel.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["archive copy cancel"] |
| `client` | ApiClient |
| `method` | DELETE |
| `operation_id` | cancel_archive_copy_job |
| `path` | /v1/archive/copies/{collection_id}/{destination_store} |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |
