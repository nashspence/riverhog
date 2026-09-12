# Operation parity: get_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection-upload-session:0b596090fe -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/53`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}](../http/get-v1-collection-upload-sessions-collection-id.md)
- [piggity collection upload show](../../piggity/cli/piggity-collection-upload-show.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [piggity collection upload watch](../../piggity/cli/piggity-collection-upload-watch.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection upload show", "collection upload start", "collection upload watch"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | get_collection_upload_session |
| `path` | /v1/collection-upload-sessions/{collection_id} |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |
