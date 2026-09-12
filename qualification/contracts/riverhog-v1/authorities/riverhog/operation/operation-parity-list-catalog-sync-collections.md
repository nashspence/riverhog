# Operation parity: list_catalog_sync_collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-catalog-sync-collections:e309737dfc -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `catalog-sync` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/22`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/catalog-sync/collections](../http/get-v1-catalog-sync-collections.md)
- [piggity catalog-sync collections](../../piggity/cli/piggity-catalog-sync-collections.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["catalog-sync collections"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | list_catalog_sync_collections |
| `path` | /v1/catalog-sync/collections |
| `provider_evidence` | None |
| `read_collection` | {"authority": "catalog-sync-bootstrap", "cursor_parameter": "cursor", "kind": "exact-authority-page", "limit_parameter": "limit"} |
| `response_authority` | canonical-document |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22d10a82c92bd18d6191764e205b850f51ffa4d2eb59bd601648c38b35bd2e78 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "catalog-sync collections"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_catalog_sync_collections",
  "path": "/v1/catalog-sync/collections",
  "provider_evidence": null,
  "read_collection": {
    "authority": "catalog-sync-bootstrap",
    "cursor_parameter": "cursor",
    "kind": "exact-authority-page",
    "limit_parameter": "limit"
  },
  "response_authority": "canonical-document"
}
```
