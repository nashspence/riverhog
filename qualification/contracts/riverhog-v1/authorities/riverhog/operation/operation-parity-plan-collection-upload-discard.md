# Operation parity: plan_collection_upload_discard

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-plan-collection-upload-discard:7ef5571968 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/57`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/discard-plan](../http/post-v1-collection-upload-sessions-collection-id-discard-plan.md)
- [piggity collection upload discard](../../piggity/cli/piggity-collection-upload-discard.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection upload discard"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | plan_collection_upload_discard |
| `path` | /v1/collection-upload-sessions/{collection_id}/discard-plan |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |
