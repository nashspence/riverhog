# RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-catalog-sync-cursor-lifetime:60c467a308 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/28`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME` — `configuration-environment:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 970506af52c3d45714af10200df6878d5e2c050b7d8e559ada0b0ebbab989055 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME"
}
```
