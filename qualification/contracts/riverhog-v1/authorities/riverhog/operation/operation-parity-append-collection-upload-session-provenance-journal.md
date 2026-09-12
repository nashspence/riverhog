# Operation parity: append_collection_upload_session_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-append-collection-upload-a8a0e8d59a:5eba4e3958 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/62`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [PATCH /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}](../http/patch-v1-collection-upload-sessions-collection-id-provenance-journals-journal-id.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | ApiClient |
| `method` | PATCH |
| `operation_id` | append_collection_upload_session_provenance_journal |
| `path` | /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id} |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |
