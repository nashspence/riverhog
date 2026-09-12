# Operation parity: create_catalog_sync_checkpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-catalog-sync-checkpoint:ac2343d3c9 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `catalog-sync` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/21`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/catalog-sync/checkpoint](../http/get-v1-catalog-sync-checkpoint.md)
- [piggity catalog-sync checkpoint](../../piggity/cli/piggity-catalog-sync-checkpoint.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | ["catalog-sync checkpoint"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | create_catalog_sync_checkpoint |
| `path` | /v1/catalog-sync/checkpoint |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af8b41ee7c4ea7c87741552923de4491d7047f4d0e63680b13da89181c557e61 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "catalog-sync checkpoint"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "create_catalog_sync_checkpoint",
  "path": "/v1/catalog-sync/checkpoint",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
