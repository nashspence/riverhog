# Operation parity: get_portable_collection_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-portable-collection-inventory:dfff9d2458 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `catalog` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/23`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/catalog/collections/{collection_id}/inventory](../http/get-v1-catalog-collections-collection-id-inventory.md)
- [piggity local add](../../piggity/cli/piggity-local-add.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | standard-tool/protocol |
| `cli_commands` | ["local add", "local repair", "local sync"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | get_portable_collection_inventory |
| `path` | /v1/catalog/collections/{collection_id}/inventory |
| `provider_evidence` | None |
| `read_collection` | {"authority": "portable-collection-inventory", "cursor_parameter": "cursor", "kind": "exact-set-page", "limit_parameter": "limit", "validator_header": "If-Match"} |
| `response_authority` | canonical-document |
