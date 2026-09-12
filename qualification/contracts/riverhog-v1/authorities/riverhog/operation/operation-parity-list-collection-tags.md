# Operation parity: list_collection_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-collection-tags:d37d7bb77f -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/86`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/collections/{collection_id}/tags](../http/get-v1-collections-collection-id-tags.md)
- [piggity collection tag list](../../piggity/cli/piggity-collection-tag-list.md)
- [piggity local add](../../piggity/cli/piggity-local-add.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection tag list", "local add", "local repair", "local sync"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | list_collection_tags |
| `path` | /v1/collections/{collection_id}/tags |
| `provider_evidence` | None |
| `read_collection` | {"authority": "collection-tag-set", "authority_parameter": "tag_set_identity", "cursor_parameter": "page_token", "kind": "exact-authority-page", "limit_parameter": "page_size"} |
| `response_authority` | http-json |
