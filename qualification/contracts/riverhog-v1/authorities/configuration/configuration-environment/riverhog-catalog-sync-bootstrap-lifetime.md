# RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-catalog-sync-bootstrap-lifetime:33cf569c58 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/27`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME` — `configuration-environment:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME"
}
```
