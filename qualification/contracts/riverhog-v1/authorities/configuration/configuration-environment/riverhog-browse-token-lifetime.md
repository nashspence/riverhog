# RIVERHOG_BROWSE_TOKEN_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-browse-token-lifetime:dfe5cfe8fc -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/25`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_BROWSE_TOKEN_LIFETIME` — `configuration-environment:RIVERHOG_BROWSE_TOKEN_LIFETIME`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_BROWSE_TOKEN_LIFETIME" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0fb099cd29921ac366335faa171608f4ca428320fd8b99c6178be930def6f8d3 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_BROWSE_TOKEN_LIFETIME"
}
```
