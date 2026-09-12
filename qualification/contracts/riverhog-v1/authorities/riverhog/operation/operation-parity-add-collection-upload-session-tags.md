# Operation parity: add_collection_upload_session_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-add-collection-upload-session-tags:7b930443b0 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/66`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/tags](../http/post-v1-collection-upload-sessions-collection-id-tags.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | ["collection upload start"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | add_collection_upload_session_tags |
| `path` | /v1/collection-upload-sessions/{collection_id}/tags |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |
