# Operation parity: get_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection:b0eb906e34 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/71`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/collections/{collection_id}](../http/get-v1-collections-collection-id.md)
- [piggity collection describe](../../piggity/cli/piggity-collection-describe.md)
- [piggity collection show](../../piggity/cli/piggity-collection-show.md)
- [piggity collection tag add](../../piggity/cli/piggity-collection-tag-add.md)
- [piggity collection tag contains](../../piggity/cli/piggity-collection-tag-contains.md)
- [piggity collection tag list](../../piggity/cli/piggity-collection-tag-list.md)
- [piggity collection tag remove](../../piggity/cli/piggity-collection-tag-remove.md)
- [piggity local add](../../piggity/cli/piggity-local-add.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection describe", "collection show", "collection tag add", "collection tag contains", "collection tag list", "collection tag remove", "local add", "local repair", "local sync"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | get_collection |
| `path` | /v1/collections/{collection_id} |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |
