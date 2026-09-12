# Operation parity: get_collection_upload_session_unit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection-upload-session-unit:39de2e49f9 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/67`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}](../http/get-v1-collection-upload-sessions-collection-id-volumes-volume-id-units-unit.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | ["collection upload start"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | get_collection_upload_session_unit |
| `path` | /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit} |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | canonical-document |
