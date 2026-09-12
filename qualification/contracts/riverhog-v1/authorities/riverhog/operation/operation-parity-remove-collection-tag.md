# Operation parity: remove_collection_tag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-remove-collection-tag:9e49a0970e -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/89`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/collections/{collection_id}/tags:remove](../http/post-v1-collections-collection-id-tags-remove.md)
- [piggity collection tag remove](../../piggity/cli/piggity-collection-tag-remove.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection tag remove"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | remove_collection_tag |
| `path` | /v1/collections/{collection_id}/tags:remove |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |
