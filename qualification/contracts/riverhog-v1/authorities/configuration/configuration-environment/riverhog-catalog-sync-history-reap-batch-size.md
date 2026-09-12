# RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-catalog-sync-history-reap-batch-size:31524c0fa6 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/29`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE` — `configuration-environment:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cfc6df069ef892e89947f3a19cd47a71863e60a8b7246773f1f2725cc058e969 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE"
}
```
