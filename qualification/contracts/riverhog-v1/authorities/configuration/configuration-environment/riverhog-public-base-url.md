# RIVERHOG_PUBLIC_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-public-base-url:0ed641ed6d -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/56`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_PUBLIC_BASE_URL` — `configuration-environment:RIVERHOG_PUBLIC_BASE_URL`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_PUBLIC_BASE_URL" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1416d7a583c2ee256c77e25eadf1c655bb5a3c695c2b01442c0b923278a1275d -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_PUBLIC_BASE_URL"
}
```
