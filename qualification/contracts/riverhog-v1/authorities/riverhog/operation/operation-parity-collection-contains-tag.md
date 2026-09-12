# Operation parity: collection_contains_tag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-collection-contains-tag:2a70c994a5 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/88`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/collections/{collection_id}/tags:contains](../http/get-v1-collections-collection-id-tags-contains.md)
- [piggity collection tag contains](../../piggity/cli/piggity-collection-tag-contains.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection tag contains"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | collection_contains_tag |
| `path` | /v1/collections/{collection_id}/tags:contains |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |
