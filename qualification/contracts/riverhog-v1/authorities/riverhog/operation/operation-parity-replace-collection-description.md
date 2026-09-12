# Operation parity: replace_collection_description

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-replace-collection-description:1d61b9e54f -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/76`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [PUT /v1/collections/{collection_id}/description](../http/put-v1-collections-collection-id-description.md)
- [piggity collection describe](../../piggity/cli/piggity-collection-describe.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection describe"] |
| `client` | ApiClient |
| `method` | PUT |
| `operation_id` | replace_collection_description |
| `path` | /v1/collections/{collection_id}/description |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |
