# Operation parity: list_catalog_sync_changes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-catalog-sync-changes:807d39cb32 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `catalog-sync` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/20`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/catalog-sync/changes](../http/get-v1-catalog-sync-changes.md)
- [piggity catalog-sync changes](../../piggity/cli/piggity-catalog-sync-changes.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["catalog-sync changes"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | list_catalog_sync_changes |
| `path` | /v1/catalog-sync/changes |
| `provider_evidence` | None |
| `read_collection` | {"cursor_parameter": "cursor", "kind": "cursor-feed", "limit_parameter": "limit"} |
| `response_authority` | canonical-document |
